# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `ox_prune --older-than` now rejects durations too large to convert or
  subtract from the current time with a `CommandError` naming the value,
  before deleting any rows (#82).
- Documented the `worker_class` structured log key on
  `claim_filter_sql_missing` and added a source-to-documentation test for
  structured-log extra keys.

### Added

- The task admin has a **Queue overview** page linked from its change list.
  It compares retained rows by queue without adding aggregation queries to
  ordinary change-list visits. The page shows status counts, eligible backlog
  and age, five-minute throughput and failure rate, and time since the last
  claim.
- `ox_worker --batch` exits once a poll pass finds nothing to claim and none
  of its own tasks is running, and `--max-tasks N` exits after N claimed attempts, for
  cron and job runners. Both drain and exit 0, log `worker_batch_empty` or
  `worker_max_tasks_reached` with the `claimed` count, and are rejected with
  `--processes` above 1. `Worker` takes matching `batch` and `max_tasks`
  keyword arguments, passed by the command only when the flag is given, so
  existing fixed-signature `WORKER_CLASS` constructors keep working when
  neither new flag is supplied.

### Fixed

- Recover outcome recording after a task encounters a dropped database
  connection, fixing a defect present in every release from 0.1.0 through
  1.4.0, and retry recording once if the dropped connection is first
  detected during the outcome write. Previously, a successful task could
  remain `RUNNING` and execute again after lease expiry; a failed attempt
  could lose its error record and intended backoff. Exhausted attempts
  could become `LOST`. Recovery retries only outcome persistence, not the
  task body, and preserves lease fencing without duplicating error records
  or applying backoff twice. Even a brief outage spanning the outcome
  write and its single retry can leave the outcome unconfirmed and require
  recovery by the reaper after lease expiry. This does not provide
  exactly-once execution or repair transaction state; recovery does not
  run inside a caller-owned transaction or with caller-disabled
  autocommit.
- Without Django's PostgreSQL pool, close the timeout watchdog's connection
  after every batch of stuck-attempt records, including failed batches.
  This prevents stale-connection reuse after a database restart or other
  disconnect between batches. Previously, a later batch could fail to record
  `TaskTimeout` and backoff, leaving recovery to the reaper after lease
  expiry. The pooled lifecycle is unchanged. An outage during a batch can
  still prevent recording.

## [1.4.0] - 2026-09-23

If you use Django's PostgreSQL pool, check PostgreSQL `max_connections`
and role connection limits before upgrading. 1.4.0 workers open renewal
connections outside the pool, in addition to `max_size`. Task timeouts (`TASK_TIMEOUT` set, or any
`TASK_TIMEOUTS` value not `None`) also run the timeout watchdog, which opens
another connection outside the pool. Each worker process can hold up to
`max_size + 1` server connections, or `max_size + 2` with task timeouts.

No migration. For `oxpull` installations, use `oxpull==1.4.0`,
which pins `django-ox==1.4.0`.

### PostgreSQL pool correction

This release fixes lease-renewal starvation affecting 0.2.0 through 1.3.1.
The watchdog path is affected from 0.3.0.

The defect affects workers using `DATABASES[alias]["OPTIONS"]["pool"]`
whose effective `max_size` per process is below `concurrency + 2`, or
`concurrency + 3` with task timeouts. `"pool": True` means `max_size` 4:
concurrency 3 or more is affected, or 2 or more with task timeouts.
Unpooled PostgreSQL, MySQL, and SQLite were not affected.

Leases can expire while task bodies still run. The reaper can reclaim that
work and start another attempt. Bodies can run again, and tasks can end as
FAILED or LOST. The defect was reproduced on 1.3.1. Earlier versions were
identified by code inspection.

To investigate past impact on pooled PostgreSQL, look in the 1.3.1 logs
for the message text "Reclaimed stuck task" and "lost its lease"
(`task_reclaimed` and `task_lease_lost` in structured logs). Where
structured fields are available, check whether the reclaimed task's
`held_by` names a worker that was still running; `worker_id` on
`task_reclaimed` names the reaper. The message "Lease renewal failed"
(`lease_renew_failed` in structured logs) with a pool-timeout traceback
containing "couldn't get a connection after N sec" confirms the cause.
Plain-text handlers may omit structured fields, so zero hits for the
structured-log keys do not rule out past impact.

These events have no fixed order. With leases well above the pool's
30-second wait, such as the 300-second default, `lease_renew_failed`
comes first. With shorter leases, `task_reclaimed` comes first.
With very short leases, `lease_renew_failed` may not appear at all.

Affected tasks can end SUCCESSFUL after two or three body runs, or end
FAILED or LOST. `queue_stats()` returns counts per queue and status, not
rows; LOST counts alone miss most affected tasks and do not establish
the cause.

When opening a private connection fails, renewal and the watchdog try the
pool with a checkout wait capped at 100 ms, then return borrowed connections.
When no private renewal connection is open and lease time is short, renewal
tries the pool first.

For 1.4.0, use at least `concurrency + 1` pooled connections per worker
process for task threads and polling, plus the outside-pool server budget
above. Include every process, alias, other client, and reserved slot.
Pool fallback needs a spare pooled connection. It adds resilience, not
capacity. Add and budget a pooled spare if fallback must work under full load.

Give old workers pools of at least `concurrency + 2`, or `concurrency + 3`
with task timeouts, before rollout, or disable their Django pool.
Keep that budget until every old worker has stopped. This is also the
workaround if you cannot upgrade yet.

See [Database connections and PostgreSQL pooling](https://oxpull.com/django-ox/production/#database-connections-and-postgresql-pooling).

### Added

- `ox_prune --format json` prints prune counts as one JSON object.
- `manage.py check` emits `django_ox.W003` when `BACKOFF_INITIAL` and
  `BACKOFF_MAX` are both explicitly set to valid numbers and the initial
  delay exceeds the cap. Retries still run and wait `BACKOFF_MAX`.
- `connection_pool_too_small` warns at WARNING level when the worker alias's
  effective pool maximum is below `concurrency + 1`. It runs once per
  `Worker.run()`. It does not resize the pool or refuse startup.
- Pooled PostgreSQL renewal reports these [events](https://oxpull.com/django-ox/monitoring/#log-events):
  - `lease_renew_degraded`: WARNING on entering degraded renewal.
    `fallback=succeeded` means renewal is using the pool; check server slots
    and connect stalls. `fallback=failed` means that tick did not renew leases.
    Alert on this event; `lease_renew_missed` can arrive after live work has
    already been reclaimed.
  - `lease_renew_fallback`: DEBUG for later successful pooled renewals.
  - `lease_renew_missed`: WARNING with missed counts, at most every 30 seconds.
    After two consecutive misses, the next tick lands at the lease boundary;
    reclaim and another run are possible.
  - `lease_renew_recovered`: INFO when private renewal resumes.
- `watchdog_connection_unavailable` warns at WARNING level once per watchdog
  pass when neither connection path is available. A pass handles the stuck
  attempts recorded together. Their outcomes go unrecorded; recycling proceeds.

### Changed

- Renewal ticks are scheduled start-to-start on every path, including unpooled
  workers. An overrun starts the next tick immediately and resets the anchor.
  Ticks do not overlap or catch up missed slots. Idle ticks still call
  `renew_leases()` once, including custom overrides; the stock idle method
  opens no connection.
- On pooled PostgreSQL, connection-acquisition failures use the new events
  instead of `lease_renew_failed`. Update alerts that relied on that event
  alone. Renewal statement failures still use `lease_renew_failed`.
  Unpooled failure reporting is unchanged.
- `watchdog_error` also reports failures while closing or returning a
  watchdog pass's connection, except that a database error while closing the
  private connection is suppressed without that event.
- The schedule admin's **Last tick** column uses a subquery instead of a
  query per row. A 100-row page runs 5 statements instead of 105.
  The displayed value and database alias are unchanged.

### Fixed

- With pooled PostgreSQL, lease renewal and the watchdog use private
  connections outside Django's pool, so pool exhaustion alone does not
  block them. If a private connection is unavailable, they fall back to
  the pool. Private connections need server capacity; pooled fallback
  still needs a spare pooled connection.
- The watchdog runs at most one acquisition sequence per pass, including
  attempts whose grace expires during acquisition or recording. It closes
  or returns the connection at the end without reconnecting between records.
  Stuck-attempt eligibility and recycling are unchanged.
- Unknown `ox_worker --backend` aliases are rejected before workers start.
  The error names the invalid alias and configured choices. Ordinary
  invocation reports one error line; `--traceback` retains the traceback.

### Limits

Nothing needs setting for renewal or the watchdog. The private renewal
connect budget is the minimum of 5 seconds, the renewal interval, and a
positive `OPTIONS["connect_timeout"]`. The watchdog uses 5 seconds or a
smaller positive configured timeout. To bound task and poll-loop connects,
set `OPTIONS["connect_timeout"]` on the database alias.

One deadline covers all hosts. A stalled first host can leave later hosts
untried. Synchronous DNS can exceed the deadline. Django's post-connect setup
queries and renewal or recording statements are not bounded by it.
These are not whole-tick or recycling deadlines.

The startup warning checks only the worker alias, not available server slots.
Runtime degraded warnings report private-path failures that startup cannot detect.

Undersized pools can still cause task-query and outcome-write failures.
After a PostgreSQL restart, tasks running at that moment can run again,
including on 1.3.1; this release does not change that. Retries and reclaims
can repeat side effects.

Task, polling, task-thread outcome-write, and hook connection paths are unchanged.
PgBouncer and third-party pools were not tested.

### Documentation

- The `db_worker` migration notes explain which options do not carry over,
  how to select one queue with `--queues default`, and why omitting
  `--queues` selects every configured queue.
- Configuration docs clarify the renewal interval and per-queue timeouts.

## [1.3.1] - 2026-09-20

No code change since 1.3.0 except the version constant.
No migration. Upgrade is optional.

### Changed

- Update the README and package summary on PyPI.

### Documentation

- Add the background tasks guide, "Why django-ox", and "Use cases".
- Update the benchmarks page with measurements against
  django-tasks-db 0.13.0 and recovery after worker death.
- Update the comparison page and correct the huey entry.
- Document how to order Oxpull Pro.

## [1.3.0] - 2026-09-17

**One migration ships with this release.** `0008_waiting` adds a status
choice and runs no SQL. django-ox never puts a task into the new status by
itself. An install that doesn't use workflows in Oxpull Pro upgrades and
rolls back as it always has.

A 1.2 process doesn't know the new status. Its admin shows a waiting task's
status as `-` and has no Waiting filter. Its `discard`, `discard_many` and
**Discard selected tasks** action skip waiting rows. Its `queue_stats()`,
`django_ox_tasks` gauge and `ox_health` don't count them. Its `get_result()`
and `refresh()` raise `ValueError` on a waiting task. Nothing in 1.2 releases
one.

**Rolling back to 1.2 after workflows have run.** A WAITING task is a task
row whose status is `WAITING`. No worker claims it, the reaper doesn't reap
it, `ox_prune` doesn't delete it, and retry skips it. django-ox writes that
status onto no row of its own, and releases no row that holds it. Something
built on django-ox does both, and on this release that is workflows in Oxpull
Pro.

Turning workflows on, and turning them off again, is Oxpull Pro configuration,
and Oxpull's own documentation has those steps. This section is the django-ox
half. See [Pro](https://oxpull.com/django-ox/pro/).

`migrate django_ox 0007` refuses while any task is WAITING on the database it
migrates. A version from before `0008_waiting` can't read a waiting task and
has nothing that would release one, so those rows would sit there for good.
The refusal reads through the connection being migrated, so waiting rows on
one alias neither block nor excuse the way back on another.

Take these steps in order, for each database that holds django-ox's tables.

1. Stop whatever writes WAITING tasks from writing more. Then wait for the
   requests, jobs and transactions that were already writing one to end.
2. Finish or cancel the work the remaining waiting rows belong to. Only what
   wrote a waiting row releases it, so django-ox can't do this part for you.
3. Stop every process that can write a WAITING task, and keep it stopped
   until it runs 1.2. That's any process that enqueues, such as web and ASGI
   processes, enqueue-only services, workers and cron jobs. A process that's
   already running keeps the settings it started with, so a settings change
   doesn't stop it.
4. On every database alias, run
   `OxTask.objects.using(alias).filter(status="WAITING").count()`. Each count
   must be 0. If one isn't, don't migrate. With the processes from step 3
   stopped, the rows left belong to work that hasn't finished or been
   cancelled. Finish or cancel it and count again. A count that goes up
   between two runs means a process that writes WAITING tasks is still
   running. Find it and stop it first.
5. Run `migrate django_ox 0007 --database alias` for each alias. It refuses
   while any task on that alias is WAITING.
6. Deploy 1.2 everywhere. Then start the processes you stopped in step 3.

The count and the migration look at the rows that exist when they run. Neither
stops a process on this release from writing a WAITING task afterwards, and
nothing at `0007` refuses one. That's why the processes from step 3 stay
stopped until they run 1.2.

A backup that holds a WAITING task is not refused by a 1.2 install.
Nothing checks, and the row restores. A 1.2 install then leaves it where it
is. No worker claims it, `ox_prune` does not delete it, and `retry` and
`discard` both return False. `get_result()` raises `ValueError`, and
`ox_health` counts the row in no column and still reports OK. Put this
release back and the row moves again, so restore such a backup into this
release or a later one.

**Django 6.1 runs the system checks against every database alias.** A
command that runs the full checks and does not name a database now checks
every alias in `DATABASES`. Checking a SQLite or MySQL alias opens a
connection to it. An alias the machine cannot reach ends the command before
it does any work, and the usual case is a replica. `runserver` is one of
these commands, so a developer whose replica is unreachable cannot start the
dev server. A reachable alias is opened too, so each extra alias costs a
connection on every such command. Django 6.0 does not do this, and nothing
in django-ox changed.

django-ox's own commands don't check an alias you didn't ask for.
`ox_prune`, `ox_health` and `ox_import_beat_schedules` each pass the alias
they work on to the checks. `ox_worker` passes an empty list, so the checks
that take a database run against nothing: a worker has to start while its
database is down and wait for it. Both are new in this release, and both hold
whether or not you pass `--database`.

For every other command, `--skip-checks` is the cheapest way out and needs
no settings change. `manage.py check` has no `--skip-checks`. Give it
`--database` and name the alias you want checked.

`--database` on its own is not the exemption. A command has to pass the
flag to the checks, and most do not. `showmigrations`, `sqlmigrate`,
`dumpdata` and `flush` all accept `--database` and still check every alias.
`SILENCED_SYSTEM_CHECKS` does not help either. The connection raises before
there is a check message to silence.

A database router is what fixes the commands that name no alias, `runserver`
among them. How far it fixes them depends on the engine. Django skips an
alias whose `allow_migrate` returns false for the model. A router that keeps
django-ox's tables on one alias therefore stops those checks reading the
others for django-ox's models. Other apps' models are still checked on every
alias. On SQLite nothing but a `JSONField` column opens a connection, so the
router is enough where django-ox holds the only ones. On MySQL every field
check reaches the server, so `contenttypes` ends the command whatever the
router says. There the router has to send every app to one alias.

A router does not cover Django's backend checks for an alias you name.
`manage.py check --database <alias>` runs them for that alias, and no router
is read on that path. On MySQL those checks ask the server for its
`sql_mode`, which opens the connection. An unreachable alias named that way
ends the command, router or no router. It's the same read behind the
`mysql.W002` warning under Changed. On PostgreSQL those checks open nothing,
so naming an unreachable alias costs nothing there. On SQLite they open
nothing either. The `JSONField` check below does, so an unreachable SQLite
alias still ends the command. It passes only behind a router that keeps
django-ox's tables off that alias. Pass `--database` only for aliases the
machine can reach.

Two field checks reach a connection. On SQLite, Django asks the alias
whether it supports `JSONField`, and the answer comes from a query. On
MySQL, the backend validates each field's column type, and reading that type
asks the server for its version. The first needs a `JSONField` column. The
second fires on the first field of the first model, whatever its type.
PostgreSQL ships no backend field validation. It answers the `JSONField`
question from a constant, so a PostgreSQL alias is unaffected.

### Added

- `OxTask.Status.WAITING`, `stats.waiting_counts()`, and the `waiting` value
  of the `status` label on `django_ox_tasks`. A waiting task reads as `READY`
  through `django.tasks`, which means it hasn't finished, not that a worker
  can take it. Workers never claim it, `ox_prune` never deletes it, and retry
  skips it. It isn't backlog, so `ready_count()`, `oldest_ready_age()` and
  `ox_health` leave it out. `QueueStats` keeps its fields, and none of them
  counts a waiting task.
- `ox_prune --queue` restricts pruning to one queue's task rows, matching
  `ox_health --queue`, so queues with different retention needs can each
  be pruned with their own `--older-than`. Old schedule ticks are still
  pruned for every schedule.
- `ox_health --format json` prints the check figures as one JSON object
  for container healthchecks and monitoring agents. On a failing check the
  object is still printed, and the exit status is unchanged.
- `--database` on `ox_prune`, `ox_health` and `ox_worker`, naming the alias
  to work on. It defaults to the alias `OxTask` writes to, the way
  `migrate --database` defaults to one. `ox_worker` names it in the command
  line of each `--processes` child, so one router answering differently in
  two processes cannot split a fleet across two databases. `ox_prune`,
  `ox_health` and `ox_import_beat_schedules` pass their alias to the system
  checks, which is what `migrate` does; `ox_import_beat_schedules` already
  had the flag and now does this too. `ox_worker` names no alias there, for
  the reason under Changed. The flag is not checked against the router. A
  worker pointed at another alias works on that one, while the admin,
  `stats` and the actions still read the router's. Nothing warns.

### Changed

- `retry_many` and `discard_many` sort the ids and lock each thousand rows in
  primary key order before they update them. That's one more statement per
  thousand rows on PostgreSQL and MySQL. A bulk retry or discard of rows
  `ox_prune` is deleting now waits for it. Before, it could fail with a
  deadlock. The call locks every row it was given, whatever its status,
  until it ends.
- When `retry_many` or `discard_many` opens its own transaction, a deadlock
  or a serialization failure starts the call again. It stops after three
  attempts in all. Inside a transaction of your own, the error still
  reaches you.
- `ox_prune` still commits batch by batch. It now locks each batch's rows in
  primary key order. A batch that hits a deadlock or a serialization failure
  runs again in a new transaction, three attempts in all. If it still fails,
  the command exits non-zero. The batches before it stay deleted, and
  running `ox_prune` again deletes the rest.
- `discard` and `discard_many` accept WAITING, and `DISCARDABLE_STATUSES`
  includes it.
- `django_ox_tasks` has a `waiting` sample for every queue, so a sum over its
  `status` label now counts waiting tasks too.
- `django_ox.stats`, `django_ox.metrics.collect()`, `render_prometheus()`,
  `render_openmetrics()` and `collector()` take `using` to name the alias to
  read. Left out, they read the alias `OxTask` writes to. On a project with
  no database router that is the same connection they always used. The
  shipped endpoint takes it from the URLconf:
  `path("ox/metrics", metrics, {"using": "replica"})` serves scrapes from a
  replica and keeps them off the primary. It's a mount argument rather than
  a query parameter, so whoever scrapes can't choose the database.
- `ox_worker` hands the system checks no database alias, so starting a
  worker opens no connection before its first poll. A worker started while
  its database is down logs `worker_poll_failed` and polls again a second
  later, on Django 5.2, 6.0 and 6.1 alike, instead of exiting. That's the
  path it already takes when the database goes away while it runs, and a
  process manager restarting a worker into a database that's still down
  gives up long before the database is back. What that costs: the system
  checks that need a database don't run for `ox_worker`. A SQLite build
  without JSON support fails `fields.E180`. `manage.py check --database
  <alias>` reports it, and so does every other django-ox command; each
  exits non-zero. A worker on that alias starts and runs tasks anyway,
  because SQLite stores those columns as text, and it logs nothing. The
  other case is a database with no django-ox tables. `check --database
  <alias>` does not report that: it exits 0 and reports no issues.
  `migrate --check --database <alias>` is what exits non-zero, and it
  prints nothing at all. The worker logs `worker_poll_failed` on every
  pass. A configuration error still stops a worker at startup, because
  those checks don't need a database.
- On MySQL, `ox_prune`, `ox_health` and `ox_import_beat_schedules` run
  Django's database checks for their alias on every invocation. On a
  connection without strict mode that prints `mysql.W002` each time,
  including from cron. Turn strict mode on, which is what the warning asks
  for, or put `mysql.W002` in `SILENCED_SYSTEM_CHECKS`.
- `ox_prune`, `ox_health` and `ox_import_beat_schedules` report a database
  they can't reach as one line, `Database unreachable: <reason>`, and exit
  non-zero. `ox_health --format json` prints its object with the figures
  null and the reason in `problems`, wherever in the run the database was
  found to be down.
- `ox_health --max-age` and `--worker-timeout` accept the duration forms
  `ox_prune --older-than` takes (`7d`, `24h`, `90m`, `45s`). A plain number
  still means seconds, fractions included.
- A worker ended by a second stop signal exits with code 130 without
  logging `second signal received; forcing exit.` first; the
  `--processes` supervisor still logs its own line.
- The source distribution no longer carries the repository's `.gitignore`. It
  carries the package, the licence, the README, the changelog and the files
  that build it.

### Fixed

- `ox_prune` and `ox_health` no longer read a replica. Under a router that
  sends reads to one, every query they made went there while their writes
  went to the primary. `ox_prune` deletes in batches, and the loop ends when
  its candidate read comes back empty. A replica that is up and behind never
  comes back empty. So the command emptied the primary and kept going: 20
  rows to 0, still running a minute later, and the operator had to kill it.
  `ox_health` answered from the replica. Over 40 READY tasks six hours old
  on the primary it printed `OK: backlog=0` and exited 0. With
  `--format json` it said `"ok": true`. A container healthcheck on it
  reported green over a queue that was stuck. `ox_prune` now finishes and
  reports what it deleted; `ox_health` reports the backlog the workers see.
  Present in every release from 0.1.0 to 1.2.0.

  Both read the alias the task rows are written to, and so does the rest
  of django-ox's reading of its own rows: `django_ox.stats`, the metrics
  renderings and the endpoint, `django_ox.actions`, `get_result()`,
  `enqueue()` and `enqueue_many()`, the worker's claim and its completion,
  the reaper, the stored schedules, and both admins.

  That list is what a test in the suite drives, under a router that
  refuses any such read on a replica. It drives the admin over HTTP,
  because calling an admin method is not the same as opening the page.
  For both models it opens the changelist and its filters, search, facet
  counts, show-all view and raw-id popup. It opens the history page and
  runs every action the admin offers. For schedules it also opens the
  change form's GET and POST, the add page and the redirect after it, and
  both delete confirmations. It calls the autocomplete endpoint another
  app's form uses to fill in a task. Those pages are what the test
  covers, and over them the sweep is a property the suite holds rather
  than a claim.

  **The admin reads the primary, and no setting changes that.** Every
  page of both admins reads the database its writes go to. Before this
  release those pages followed the read alias, so a router that splits
  reads sent them to a replica. On a replica that lags, the schedule
  admin's add page then raised `OxSchedule.DoesNotExist` on the row it
  had just written. The load now lands on the database your workers use.
  If you were serving admin reads off a replica on purpose, this ends it.

  That is the right default, because the admin is not a reading page. It
  writes back what it read. A change form submits every field, including
  the ones nobody touched, so a form built from a replica overwrites
  newer values on the primary. Nothing raises and nothing is logged.

  What you can still point at a replica is what you ask for by name.
  `django_ox.stats`, `collect()`, the renderings and the endpoint take
  `using`, so `path("ox/metrics", metrics, {"using": "replica"})` keeps
  scrapes off the primary. Your own queries are untouched: a router you
  wrote still sends your reads of the task table where you send them.
  [Configuration](https://oxpull.com/django-ox/configuration/#read-replicas)
  covers what django-ox pins and two places it cannot reach.
- A worker no longer loses a task's result under a router that sends reads
  to a replica. On MySQL and MariaDB every claim re-reads the row it has
  just claimed, and that read followed `db_for_read`. The worker was handed
  the row as it stood before the claim, so it ran the task holding a lease
  the row no longer had: its own finish write matched no row, the result
  was lost, and the row sat RUNNING until the reaper requeued it. The
  re-read now names the alias the claim was written to. PostgreSQL claims
  in one statement and reached this only through a subclass that overrides
  `claim_filter_q()` without `claim_filter_sql()`. SQLite takes the
  compare-and-set path and was never affected.
- Stored schedules read the database they are written to. Under a router
  that sends reads to a replica, creating or editing a schedule checked the
  name against the replica, so a name already taken on the primary passed
  validation and the INSERT raised `IntegrityError`, which the admin showed
  as a server error rather than as "Schedule with this Name already
  exists." `update_schedule()` and the admin's add page then read the row
  back from the replica: a row the replica had not seen yet raised
  `OxSchedule.DoesNotExist` after a write that had succeeded, and an older
  one came back holding the values that write had just replaced. The
  **Enable**, **Disable** and **Run selected schedules once now** actions
  read their rows from the replica too, so a manual run could carry
  arguments that had already been changed. All of it now reads the alias
  the schedules are written to. The unique index on the name is unchanged:
  a check can't win a race against an insert that commits between the check
  and the write, so the index is what makes the name unique and the check
  is what turns the ordinary duplicate into a field error.
- The schedule admin no longer writes a stale copy of a row over a newer
  one. Under a router that sends reads to a replica, Django built the
  changelist and the change form from the replica. A change form submits
  every field, so **Save** wrote the replica's values back over the
  primary's. There was no error and no warning, and the newer values were
  gone. `update_schedule()` takes the row lock and writes only the
  submitted fields to stop this, and a stale form walked through it.

  Two more followed from the same read. **Save and continue editing** on
  the add page looked the new row up on the replica. The person was told
  the schedule they had just made doesn't exist, and landed on the admin
  index. **Delete selected schedules** counted the rows on the replica,
  found none, and returned before it reached django-ox's own delete. It
  deleted nothing and said nothing at all.

  Both admins now read the alias their rows are written to, and so does the
  **Last tick** column. The task list, its filters and a task's page read
  the replica too. A task enqueued a moment ago was missing from the list,
  and a task's own page reported it as deleted. The schedule pages are
  present since 1.2.0, which added stored schedules; the task pages since
  0.3.0, which added them.
- `ox_prune --include-failed` no longer deletes a row that an operator retries
  while its batch is being deleted. The DELETE matches rows by primary key
  alone, so a FAILED or LOST row retried just before it ran was deleted anyway.
  The retry still reported success, in the admin and in `django_ox.actions`. A
  row discarded at that point was deleted too. Each batch is now checked again
  inside the transaction that deletes it. Only rows that still qualify are
  deleted, and nothing else can write to them until that transaction ends. A
  retry that reports success now keeps its row. Without the flag, `ox_prune`
  was not affected.
- `retry_many` and `discard_many` open their transaction on the database
  that `OxTask` writes to. Under a router that sends `OxTask` to another
  database, each UPDATE committed by itself. An error part-way could leave
  some rows moved.
- The lease documentation put the renewal margin at two consecutive missed
  renewals. It is one. The renewal loop waits `LOCK_TIMEOUT / 3` after each
  renewal rather than firing on a fixed schedule, so every round costs the
  wait plus the UPDATE that renews. Three rounds therefore always come to
  more than the lease, and a second miss in a row leaves the lease expired
  and the task reclaimable. Nothing in the worker changes. Size
  `LOCK_TIMEOUT` for one missed renewal.
- `ox_health --max-age` and `--worker-timeout` accepted `nan`, `inf` and
  numbers that round to `inf`. A threshold set to one of those could never
  be exceeded, so that check could not fail. It passed however old the
  oldest task waiting to run or the last claim was. `--max-backlog` takes
  an int and was not affected, and neither was the `--worker-timeout`
  branch that reports no claim at all. They are refused as usage errors
  now.
- A stop signal could leave an idle `ox_worker` hung instead of draining.
  It stayed hung until a second signal or the process manager ended it,
  or for good when it was a worker process whose supervisor had been
  killed. A worker that has finished starting now drains on the signal.
- A worker process whose supervisor died while the worker was still
  starting ran on as an orphan. It now drains and exits having claimed
  nothing.
- If the `--processes` supervisor hit an error while running, a second
  stop signal did not send SIGKILL to a worker process that would not
  exit, and the supervisor waited for it forever. The second and third
  signals now escalate as they do in any other stop.

## [1.2.0] - 2026-09-12

**One migration ships with this release.** `0007_oxschedule` creates two
tables, `django_ox_oxschedule` and `django_ox_oxschedulechange`, with a unique
index on the schedule name and a check constraint. It touches neither the task
table nor the tick log, so there is no index build on a table workers are
reading: on PostgreSQL the locks are on the new tables only, inside one
transaction, and on MySQL each `CREATE TABLE` takes a metadata lock on its own
new name. Enqueues, claims and the dispatch of settings schedules carry on
through it, and a 1.1.0 worker keeps running beside a worker on this release
once it is applied. Migrate before starting any worker whose backend names
`django_ox.stored.DatabaseScheduleSource`, and before opening the schedule
admin: both read the new tables.

One thing a rolling deploy does not close. A settings schedule that has no
tick yet and is first seen during the rollout can be anchored twice, once by
a 1.1.0 worker and once by a worker on this release, when their passes hold
different ticks: the 1.1.0 worker does not take the first-sighting latch this
release adds, so the latch serialises only the new workers. The later of the
two ticks is then recorded with no task and does not fire. That is the race
1.1.0 has between two of its own workers, not one this release introduces,
and it ends when the last 1.1.0 worker stops. Once a schedule has any tick,
both versions coordinate on the tick log's unique index and nothing anchors
again. Stored schedules are not affected: a row carries its boundary and never
anchors.

Migrating back to `0006` drops both tables and every stored schedule with
them. The tick log keeps its rows, including those named `db:<id>` for
schedules that no longer exist. A worker on this release that is still
running warns on every pass that it cannot read the schedule tables, and
keeps dispatching the settings schedules from the set it last read; stop it,
or migrate forward again. `sqlmigrate django_ox 0007` prints the statements
for your engine.

### Added

- `OPTIONS["SCHEDULE_SOURCE"]`, a dotted path to the class a worker asks for
  its active schedules on every dispatch pass. It defaults to reading
  `OPTIONS["SCHEDULES"]`, so settings-declared schedules are unchanged. A
  source owns its own freshness, which is what lets one read somewhere that
  changes without the worker knowing. `django_ox.E006` reports a source that
  cannot be built or has no `schedules()` method.
- Fixed-interval schedules. A `SCHEDULES` entry takes `every` instead of
  `cron`, as a `timedelta` or a number of seconds, with an optional `phase`
  to offset the sequence. Exactly one of `cron` and `every` is required.
  Ticks are counted from a fixed instant rather than from the last run, so a
  restart, a pause or an edit cannot shift the cadence: every worker derives
  the same instants from the definition alone, which is what keeps dispatch
  leaderless. `every` must be at least one second, because the dispatch loop
  cannot honour anything faster.
- A tick whose instant has not arrived is not enqueued. On the day a zone
  springs forward, an hour of wall-clock labels never happens, and a label
  inside it resolves to an instant on the far side of the gap. That tick is
  held until its instant arrives and fires once.
- A registry of the tasks a schedule may name. `@schedulable("reports.daily")`
  above `@task` exposes a task under a key, and `OPTIONS["SCHEDULABLE_TASKS"]`
  does the same from settings, which is the only channel a system check can
  see. It is the boundary the stored schedules rest on: a row names a key the
  code owns rather than a dotted path anyone with the change permission could
  choose. Registration is discovered lazily on first use, so a project not
  using the registry imports nothing it did not already import. An entry may
  declare a `permission`, checked in addition to the model permissions before
  a schedule naming that key is written, when a `user` is passed; it is
  enforced in `django_ox.stored`, not only in the admin. `django_ox.E007`
  reports a bad entry.
- `django_ox.registry.ArgsForm`, a Django form for a schedule's arguments
  that closes two defaults written for HTML posts rather than stored rows: an
  unknown argument is an error instead of being ignored, and a text field
  refuses a non-string instead of coercing it, so `5` cannot reach a task as
  `"5"`.
- `OxSchedule`, a recurring schedule stored as a database row so it can be
  created, retimed and paused without a deploy, and `OxScheduleChange`, the
  single row a worker reads to know whether the stored schedules moved. A row
  names a registry key rather than an import path. It carries `start_time` as
  its activation boundary, written when the row is created rather than when a
  worker first notices it, and records the timing and pause state that
  boundary was set for, so retiming a schedule reschedules it from the moment
  of the change instead of firing a tick that already passed. Because that
  record is derived from the row rather than incremented by a write path, a
  retime or a pause made with a bulk `update()` is noticed too, at the next
  read, and the boundary moves to that read: the worker remembers the
  boundary it found the row stale against, so a row re-enabled the same way
  before the move is written still gets it. It keeps that memory until the
  move commits, so a dispatch pass run inside a transaction the caller rolls
  back has forgotten nothing by its next one. However many workers find the row
  stale together, only the first of them moves the boundary: the row counts
  its boundary writes and each worker records the count it saw, because a heal
  writes the boundary to its own clock, and writing it is not the same as
  changing it. A change made and reverted between two reads is not, and the
  schedules page says what that means for a bulk pause and resume. Ticks are
  recorded against the row, as `db:<id>`, so renaming a schedule keeps its
  history; a settings-declared schedule may not use that prefix, and
  `manage.py check` refuses one that does.
- `start_time` and `end_time` bound which ticks of a stored schedule fire, and
  `starting_deadline_seconds` drops a tick that is later than the deadline
  rather than running it however stale. The deadline is judged under the
  row's lock, after any wait for it, so a tick that crossed it while another
  worker or an admin save held the row is dropped rather than run late. The
  default is no deadline, which is the behaviour settings-declared schedules
  have always had. A dropped tick logs `schedule_tick_dropped` with its
  lateness, once per worker, so it can be alerted on instead of vanishing.
- `django_ox.stored.DatabaseScheduleSource`, which dispatches the stored
  schedules alongside any `SCHEDULES` entries. Name it in
  `OPTIONS["SCHEDULE_SOURCE"]` and a row can be created, retimed and paused
  without a deploy or a restart. Rows are re-read when the change row moves,
  and in full every `OPTIONS["SCHEDULE_RECONCILE_INTERVAL"]` seconds (default
  60) as the backstop for a row written without `django_ox.stored`, so the
  steady state is one read of one row per dispatch pass. A row that no longer
  validates is skipped and logged as `schedule_row_skipped`; the others still
  run. When the rows cannot be read at all, the worker logs
  `schedule_source_unavailable`, with the database's own message and no
  traceback, and keeps dispatching the set it last read.
- A schedule disabled, retimed or deleted after a worker read it does not
  fire. The check runs inside the dispatch transaction, under the row's own
  lock, because no polling interval is short enough to close that window. It
  is taken with a locking read where the database has one, and on SQLite by
  making the transaction a writer before it reads, because
  `select_for_update()` there is a silent no-op. A refused tick commits
  nothing.
- `django_ox.stored.create_schedule`, `update_schedule` and
  `delete_schedule`, the supported programmatic write path. They validate,
  maintain the boundary and bump the change row. They take the fields a
  schedule's author owns and refuse the rest with a `TypeError` naming what
  they do take: the activation boundary, the count of writes to it and the
  two timestamps are theirs to maintain, and a misspelled field name is
  reported rather than set on the instance and silently not saved.
  `create_schedule` still takes `start_time`, which is a new schedule's
  boundary. `save()` does not call
  `full_clean()`, so a `clean()` method alone would validate what the admin
  submits and nothing that `objects.create()` writes. A valid row written
  that way still runs, with its boundary moved to the read that found it and
  logged as `schedule_boundary_healed`.
- A Django admin for stored schedules, the first place the package lets a
  person author a row rather than act on one a worker wrote. The task field
  is a choice drawn from the registry, so it cannot
  express a task the code has not exposed, and the same membership check runs
  again on the model for the write paths that build no form. A registry
  entry's own `permission` comes back as an error on that field, with the
  submission intact, rather than as a bare 403 that discards it; the
  permission itself is enforced in `django_ox.stored` as before, on every
  write path. Saving routes
  through `django_ox.stored`, so a schedule retimed in the admin gets its
  activation boundary moved rather than keeping one set for its old timing.
  Actions enable, disable, and run a schedule once immediately; a manual run
  writes no tick row, so the next scheduled tick still fires. It is also the
  one way to run a paused schedule: it ignores both `enabled` and `end_time`,
  and says how many of the schedules it ran were disabled or already ended, and
  how many it was refused by a registry entry's own permission.
  The changelist carries `end_time`, so a row that has ended is visible where
  the selection is made. The backend a manual run enqueues through is found the
  way the worker finds it: the class `OPTIONS["SCHEDULE_SOURCE"]` names is
  built and asked whether it answers `schedules()`, so any source the worker
  would dispatch from is found, whatever it is called and whatever it
  inherits, and a subclass is the class the run builds its schedule with.
  The changelist and the add page say so when no backend names a source at
  all, because until one does,
  a schedule saved there is stored and never dispatched, with nothing else
  to say so: no error, no log, no system check, and a manual run that
  enqueues anyway.
- `manage.py ox_import_beat_schedules`, which reads a `django-celery-beat`
  schedule table and prints the django-ox equivalents. It writes nothing, names
  what it could not translate and why, and says which timing will differ.
- `django_ox.E008` reports django-ox models routed to more than one database.
  A task row and its tick row commit together, which is what makes a due tick
  enqueue once, so they must share a database. Routing the app to a single
  non-default database is supported.
- `django_ox.E009` and `django_ox.W002` report two configured schedule names
  that differ only by case. Tick identity is decided by the column's
  collation, and MySQL's default folds case, so the two share one key: their
  ticks collide and one schedule stops running with nothing raised. Refused on
  MySQL, warned about elsewhere.
- `django_ox.W001` reports `USE_TZ = False` together with a `TIME_ZONE` that
  puts the clock back once a year. A tick's time is stored as a wall clock
  there, so one label covers both passes of the repeated hour: an interval
  schedule loses about half its runs for the length of it, and a cron schedule
  inside it fires once rather than twice. The schedules page states the effect.
- `schedule_lock_unavailable`: a lock the database gave up waiting for,
  MySQL's lock-wait timeout or a deadlock it resolved against this worker,
  SQLite's busy timeout, PostgreSQL's `lock_timeout`, is logged once without
  a traceback, and the schedule is skipped this pass; its tick fires on a
  later pass if still unclaimed. In 1.1.0 the same timeout on a
  settings-declared schedule's tick ended the whole poll pass as
  `worker_poll_failed`, with a traceback, and the claim did not run that
  pass.

### Fixed

- A settings-declared schedule anchors once, however many workers first see it
  and whatever tick each of them holds. Two workers whose first passes fell
  either side of a minute boundary, or whose clocks differed by one, each
  wrote an anchor: their rows had different tick times, so the unique
  constraint serialised neither, and neither read saw the other's
  uncommitted row. The later instant was then claimed with no task, and the
  run it was for never happened. A pass that reads no history now takes a
  per-schedule latch inside its transaction before it decides, so the second
  worker waits, sees the anchor, and fires. The latch is a tick row at an
  instant no trigger produces, 1900-01-01, written and deleted inside the
  transaction, so nothing ever reads it. Present in 1.1.0.
- The tick read at the start of a dispatch pass fits the database's
  parameter limit. It names every schedule in one `IN` list, and SQLite
  before 3.32.0 refuses a statement with more than 999 parameters, so a
  deployment with that many schedules on SQLite 3.31 dispatched nothing,
  every pass. The keys are now read in slices of what the connection allows;
  PostgreSQL, MySQL and current SQLite declare no limit and read them in one
  statement as before. Present in 1.1.0.
- An enqueue that fails with an integrity error is reported as
  `schedule_dispatch_error`. In 1.1.0 every integrity error inside the
  dispatch block was read as a lost race with another worker, so an enqueue
  that kept failing was retried silently on every tick. The two are now told
  apart by whether this pass's own tick row had gone in.
- A `transaction.on_commit` callback that raises after a dispatch commits is
  logged as `schedule_dispatch_callback_failed`, with the task id, and the
  dispatch is counted, since the task exists and the tick is recorded,
  whatever the callback raised: a database error from a callback's own
  statement is the callback's too, not a lost race or a fault that ends
  the pass. In
  1.1.0 the exception left the dispatch pass with the task committed and,
  unless it was a database error, left `run()` too and stopped the worker.
  The stored-schedules page states the lock order a `task_enqueued` receiver
  must respect: it runs inside the dispatch transaction, under the schedule
  row's lock.

### Changed

- A database error inside the dispatch pass ends the pass, logged once as
  `schedule_dispatch_failed`; a connection that is no longer usable is
  dropped, and the claim still runs on a fresh one. In 1.1.0 the same error
  ended the whole poll pass as `worker_poll_failed` and the claim waited for
  the next one. Anything else one schedule raises is logged against that
  schedule as `schedule_dispatch_error` and the rest of the pass continues.
- Schedule dispatch runs on every pass whether or not a schedule is
  configured, because a source that reads the database can gain one at any
  time. With nothing configured it returns on a list check, before any query.
- The recurring-tasks page states what the tick log guarantees. It
  deduplicates the enqueue: a unique constraint on (schedule name, tick time)
  stops two workers enqueueing the same tick. It does not make a task run
  exactly once, and it does not enqueue every cron occurrence, because only
  the latest missed tick is enqueued after an outage and a new schedule never
  fires for a time before it existed. The README, the production page and
  the agent-facing files say the same.

## [1.1.0] - 2026-09-11

**Two migrations ship with this release.** `0005_dequeue_index` rebuilds the
index the claim reads and adds a second one; `0006_lease_expiry` adds a
nullable column and an index on it. Migrate before rolling any process that
imports django-ox 1.1.0, web processes included: `enqueue()` writes the column
0006 adds.

On PostgreSQL, `CREATE INDEX` takes a lock that blocks enqueues and claims for
the duration; MySQL 8 builds a secondary index online. On a large PostgreSQL
task table, build the indexes by hand and fake the migrations, in an order
that keeps the claim indexed throughout. Build the replacement
`ox_dequeue_idx` under a temporary name, swap it in, then add the other two:

```
CREATE INDEX CONCURRENTLY ox_dequeue_idx_new
    ON django_ox_oxtask (status, priority DESC, enqueued_at);
DROP INDEX CONCURRENTLY ox_dequeue_idx;
ALTER INDEX ox_dequeue_idx_new RENAME TO ox_dequeue_idx;
CREATE INDEX CONCURRENTLY ox_dequeue_queue_idx
    ON django_ox_oxtask (status, queue_name, priority DESC, enqueued_at);
ALTER TABLE django_ox_oxtask
    ADD COLUMN lease_expires_at timestamp with time zone NULL;
CREATE INDEX CONCURRENTLY ox_reaper_expiry_idx
    ON django_ox_oxtask (status, lease_expires_at);
```

then `migrate --fake django_ox 0006`. The `ADD COLUMN` is nullable with no
default, which is a catalogue change. `sqlmigrate django_ox 0005` and
`sqlmigrate django_ox 0006` print the same statements without
`CONCURRENTLY`, for checking against your settings.

### Added

- `lease_expires_at` on the task row: when the lease stops being valid,
  written by the worker that took it and refreshed on every renewal. Every
  reaper judges that column instead of deriving a deadline from its own
  `LOCK_TIMEOUT`, so the setting can be changed in a rolling deploy.

  A row claimed before the column existed has it empty, and the reaper keeps
  comparing `locked_at` against its own timeout for those. The first renewal
  after the upgrade fills it in, so a fleet converges lease by lease with
  nothing for an operator to run. An expiry older than the row's own
  `locked_at` was left there by a worker that does not know the column and
  counts as absent too, so with `LOCK_TIMEOUT` unchanged across the rollout
  a 1.0.0 worker that re-claims a row is judged on `locked_at`.
- `django_ox.actions.expire_lease(result_id)` expires a RUNNING task's lease so
  the next reaper pass reclaims it. The row carries its own deadline now, so a
  lease granted with a timeout that turned out to be wrong outlives the
  configuration that granted it; this is how to shorten one. It does not stop
  the task.
- `lease_expires_at` appears in the admin's Lease fieldset.

### Fixed

- A worker that renews its lease late, but renews, keeps its task. The reaper
  checks the expiry in the reclaim itself, against the same cutoff it selected
  with, so only a lease still stale at the moment of the write is taken back.
- The lease renewal thread survives a dropped connection. It discards the
  connection, reconnects on the next interval, and keeps `locked_at` moving on
  every row the worker holds.
- The poll loop survives a database error. The pass is abandoned, logged as
  `worker_poll_failed`, and retried on the next one with a fresh connection,
  so one blip is not a worker death and cannot reach the supervisor's restart
  cap.
- The timeout watchdog handles each stuck attempt on its own. A failure
  recording one is logged as `watchdog_error` and every other armed attempt
  keeps its deadline, and the recycle that frees the worker happens whether
  or not the record was written.
- A worker whose timeout backstop gives up on a thread recycles on whether
  that thread is still running, not on whether its own outcome write landed.
  The pool slot is freed and the drain does not wait on the thread.
- The drain waits for healthy work and stops waiting for abandoned work. It
  counts threads still inside the attempt that was abandoned, rather than
  pool threads that happen to be alive, and it observes a backstop that fires
  part way through an ordinary drain.
- The drain is safe to run while a second attempt goes stuck: the stuck set
  is written under the same lock the drain reads it under.
- Every statement in the claim protocol runs on the database the worker
  writes to, including the raw PostgreSQL claim, so a read replica cannot
  answer a question that decides who holds a task.
- `enqueue_many` opens its transaction on the connection `OxTask` routes to,
  so its all-or-nothing guarantee holds under a database router.
- A worker subclass that overrides `claim_filter_q()` alone takes the claim
  path that applies it, on every database. It logs
  `claim_filter_sql_missing` once to say it gave up the single-statement
  PostgreSQL claim to do so.
- The PostgreSQL claim, the renewal and the reaper stamp and judge the lease
  on one clock: the database's with `USE_TZ` on, the worker's with it off.
- The reaper requeues abandoned rows in one UPDATE per pass, up to
  `reap_batch` rows at a time, and retires rows whose attempts are spent in
  the same bounded batches. Its cost per pass is bounded whatever the size of
  the stuck set.
- The reclaim record names only tasks the pass reclaimed. A pass
  whose stuck set changed while it ran reports a `count` and names nobody.
- The claim reads its candidate out of an index, in order, for a worker that
  names one queue, several, or none. Two index shapes ship and the planner
  picks per query. Migration `0005_dequeue_index` builds them.
- Dispatching schedules reads only the ticks it is asking about: the tick log
  is bounded to the oldest due tick across the configured schedules, which the
  unique index can seek to, and the read is pinned to the database the worker
  writes to.
- The tick row is written before its task is enqueued, so on every tick
  exactly one worker enqueues and the others announce nothing. A tick already
  in the log is not dispatched again whatever a fast-clocked worker recorded
  after it, and `task_enqueued` fires only for a task that exists.
- A signal receiver that raises is not charged to the task. A `task_started`
  receiver's exception does not spend an attempt, and a `task_enqueued`
  receiver's exception does not surface from `enqueue()` over a task that is
  already committed.
- A task run through `run_once()` keeps its lease renewed for the duration,
  the same as a task on the pool.
- `KeyboardInterrupt` and `SystemExit` reach the caller when a task runs
  through `run_once()`. On the worker pool they are recorded as a failed
  attempt, so one task calling `sys.exit()` cannot stop a fleet.
- The retry delay stays in range for any `MAX_ATTEMPTS`. The doubling is
  capped before the multiplication, so the failure path never raises on the
  arithmetic.
- `LOCK_TIMEOUT`, `BACKOFF_INITIAL` and `BACKOFF_MAX` are validated at
  `manage.py check` as `django_ox.E010`, by the same rule as the timeout
  options: a positive, finite number of seconds.
- Each stored traceback is capped at 16,384 bytes, marker included, with both
  ends kept. One failure cannot write an unbounded string onto its own row.
- `django_ox.remaining()` is measured on the monotonic clock the timeout is
  enforced with, so a clock correction cannot put the two on different sides
  of the same instant. `django_ox.deadline()` still answers with a wall-clock
  time.
- `manage.py ox_worker` starts on Windows. Stop signals are built from the
  ones the platform has; `--processes` above 1 still refuses off POSIX with
  its usual message.

### Changed

- A recycling worker now bounds how long it waits for its other in-flight
  tasks, at `LOCK_TIMEOUT`. Past that it stops renewing their leases and exits,
  and the reaper requeues them. This can end a task that was going to finish:
  it is recorded as a lost lease and retried, and one on its last attempt
  reaches LOST. The bound is what makes a recycle certain on a process that
  has stopped trusting one of its threads.
- `MAX_ATTEMPTS` and the `attempt` log key count claims, not invocations. The
  behaviour is unchanged; the reference table now says so.
- Five log events that only fire while something is wrong are documented:
  `worker_poll_failed`, `watchdog_error`, `task_stuck_unrecorded`,
  `worker_drain_abandoned` and `claim_filter_sql_missing`.
- `task_reclaimed` carries `held_by`, the worker that stopped refreshing the
  lock. `worker_id` on the same record is the reaper that noticed.
- SQLite guidance: run one worker and give it threads with `--concurrency`,
  rather than several worker processes. `SKIP LOCKED` is what lets workers step
  past each other's rows, and a database without it hands out the head of the
  queue one worker at a time.
- `task_reclaimed` is still one record per task on the ordinary path. When the
  stuck set changes underneath a reap pass (a lease renewed, or one more
  expired), that pass emits a single record carrying `count` and no `task_id`,
  rather than naming tasks it cannot vouch for.
- The production page says which databases give the lease one shared clock.
  SQLite computes `Now()` inside the process that runs the statement, so every
  worker uses its own clock there whatever `USE_TZ` says; run SQLite on one
  host. With `USE_TZ` off the lease is on the worker's clock on every
  database, so keep worker clocks within two thirds of `LOCK_TIMEOUT` of
  each other, the timeout less the renewal interval.

## [1.0.0] - 2026-09-05

This release marks django-ox as production ready. The public API is stable from
here: anything documented keeps working until a 2.0, and breaking changes get a
major version. The package runs on Django 5.2 LTS through 6.1, on SQLite,
PostgreSQL and MySQL, and the suite covers all of them.

### Added

- Django 5.2 LTS support. Django ships the Tasks framework in core from 6.0;
  on 5.2 the same framework comes from the `django-tasks` backport, and a new
  `backport` extra pulls it in: `pip install "django-ox[backport]"`. Every
  import of the framework now goes through `django_ox.compat`, which picks
  core or the backport at runtime, so nothing else in the package changed.
  CI runs the whole suite on 5.2 against the backport, on SQLite, PostgreSQL
  and MySQL.
  Python 3.14 is not in the 5.2 legs, since Django 5.2 does not support it.
- `Django>=5.2` replaces `Django>=6.0` as the declared dependency, and
  `Framework :: Django :: 5.2` joins the classifiers.

### Changed

- `finished_at` is stamped from the process clock instead of being computed by
  the database and read back, so an outcome write is one statement. The lease
  is untouched: `locked_at` and the reaper's cutoff stay on the database
  clock, which is where a lease is judged.
- The `backport` extra requires `django-tasks>=0.12`. Earlier backport releases
  change the framework API in ways django-ox does not support: 0.9.0 has no
  `enqueue_on_commit` on `Task`, 0.10.0 rejects the result statuses django-ox
  stores, and 0.11.0 adds an abstract `save_metadata` that the backend does not
  implement. CI now installs the floor with `==` and asserts the resolved
  version.
- `Development Status :: 5 - Production/Stable` replaces the beta classifier.

## [0.4.0] - 2026-09-01

### Added

- `WORKER_CLASS` in a backend's `OPTIONS`: the dotted path of the `Worker`
  subclass `ox_worker` runs. Resolved from settings rather than from the
  command line, so a worker chosen here is the worker in every child
  process under `ox_worker --processes N`, not only at `--processes 1`.
- `Worker.claim_filter_q()` and `Worker.claim_filter_sql()`, two hooks a
  subclass overrides to narrow what it may claim. The condition is applied
  inside the candidate select on all three claim paths, ahead of its
  ordering and its limit, so a narrowed worker still claims runnable work
  behind rows it declines. `claim_filter_q()` covers the two paths that
  build a queryset and `claim_filter_sql()` the PostgreSQL claim, which
  builds its own SQL, so implement both. The defaults are `None` and an
  empty fragment, so the emitted SQL is unchanged without them.

## [0.3.1] - 2026-08-24

### Fixed

- `ox_worker --processes N` exits 0 when a stop signal arrives while a
  worker process is still starting up. A worker cannot act on a signal
  until it has installed its handler, and that is after Django has been
  imported. A signal landing before then killed the worker outright, and
  the supervisor reported that worker's 143 as its own exit code. A unit
  on `Restart=on-failure` reads that as a fault and starts the service
  again. The worker had claimed no work, so there is nothing to report:
  the supervisor now logs `worker_process_stopped_early` and exits 0.

### Changed

- A worker enforces `TASK_TIMEOUT` on a sync task through the grace backstop
  alone while a coverage tool or a debugger is watching the thread the
  attempt runs on. Two things count: a trace function (`sys.settrace`, which
  `coverage run` installs before Python 3.14, and which `pdb` and most
  debuggers install), and a registered `sys.monitoring` tool with events
  enabled (which `coverage run` uses from Python 3.14 on). Nothing is raised
  inside the task. A task that returns within `TASK_TIMEOUT` plus
  `TASK_TIMEOUT_GRACE` is recorded as whatever it did, however long it ran,
  with no `task_timed_out` event; one still running then is recorded as
  failed and recycles the worker. An async task is cancelled at its deadline
  either way, and a profile hook (`sys.setprofile`) is not consulted.
- The worker logs `timeouts_backstop_only` once under such a tool, on the
  first attempt it registers rather than at startup. That event now carries
  `reason` (`interpreter` or `tracing_tool`) and, for a tool, `tracer`:
  `sys.settrace`, or `sys.monitoring (NAME)`.

## [0.3.0] - 2026-08-24

### Upgrading

- Run `python manage.py migrate django_ox`, and roll every process, web and
  worker, to 0.3.0 before the first discard. This release adds the
  DISCARDED status: a 0.2.1 process that reads a DISCARDED row raises
  `ValueError` from `get_result()` and `refresh()`, and 0.2.1's `ox_prune`
  cannot delete such rows. Rolling back with DISCARDED rows present keeps
  that crash until the rows are removed by hand
  (`DELETE FROM django_ox_oxtask WHERE status = 'DISCARDED'`); reversing
  the migration does not remove them.

### Added

- `TASK_TIMEOUT`, `TASK_TIMEOUTS` and `TASK_TIMEOUT_GRACE` backend options, a
  limit on how long one attempt may run. At the deadline the worker raises
  `django_ox.exceptions.TaskTimeout` inside the task, on the task's own
  thread, so `finally` blocks run and an open `transaction.atomic()` rolls
  back; an async task is cancelled inside its event loop instead. The attempt
  is recorded as failed with the `TaskTimeout` error and retried on the usual
  backoff, or marked FAILED when attempts are spent. A thread that has not
  stopped `TASK_TIMEOUT_GRACE` seconds later (default 30) is treated as
  stuck: the worker records the attempt as failed, moves the lease number so
  the thread can write nothing to the row, stops claiming, drains its other
  tasks and exits with code 75, which `--processes` restarts without counting
  it against the restart cap. `TASK_TIMEOUTS` maps a queue name to its own
  value, and a key that is not in `QUEUES` fails `manage.py check` as
  `django_ox.E005`; a bad value fails it as `django_ox.E004`. Off by default.
  `django_ox.deadline()` and `django_ox.remaining()` read the attempt's
  deadline from inside a task. `TaskTimeout` subclasses `TimeoutError`. New
  log events: `task_timed_out`, `task_stuck`, `worker_recycling`,
  `worker_process_recycled` and `timeouts_backstop_only`.
- `ox_worker --processes N`. Above 1, the command supervises N copies of
  itself, each a full worker with its own database connections, lease renewal,
  reaper and `--concurrency` thread pool, so `--processes 2 --concurrency 4`
  runs eight tasks at once. Every worker id ends in its slot number. SIGTERM,
  SIGINT or SIGHUP to the supervisor is forwarded once and the supervisor exits
  0 when every worker drained or recycled, or with the first other non-zero
  code; a second signal is the force-exit, and a worker that has not exited
  five seconds later is SIGKILLed. A worker process that dies is restarted with
  `worker_process_restarted` at WARNING, after one second and then with a
  doubling delay up to 30 seconds; a worker that recycled itself, exit code 75,
  comes back after one second under `worker_process_recycled`, outside the
  death count and the backoff. More than five deaths of one slot in a minute
  stops the supervisor with exit code 1 and `supervisor_restart_cap` at ERROR.
  A worker whose supervisor dies drains and exits (`worker_orphaned`). The
  children are started the way the supervisor was, `manage.py` by absolute path
  or `python -m django`, with `--settings` and `--pythonpath` passed on, so the
  command works from any working directory. `--processes 1`, the default, is
  the worker as before. POSIX only.
- A Prometheus endpoint. `path("ox/", include("django_ox.urls"))` mounts
  `GET /ox/metrics`, which renders the `django_ox.stats` numbers as gauges in
  the Prometheus text format (OpenMetrics on request), from the standard
  library alone. The metric names are `django_ox_tasks{queue,status}`,
  `django_ox_ready_tasks`, `django_ox_oldest_ready_age_seconds`,
  `django_ox_last_claim_age_seconds`, `django_ox_throughput_per_minute` and
  `django_ox_failure_rate`, and they are public API from this release. The
  view has no authentication of its own. `django_ox.metrics.collector()`
  returns a collector for a `prometheus_client` registry when that package is
  installed; it is not a dependency.
- `django_ox.actions.retry(result_id)` and `django_ox.actions.discard(result_id)`.
  A retry puts a FAILED or LOST task back to READY for one more attempt, keeping
  its attempt count, worker ids and every traceback, and clearing the backoff so
  it is eligible at once. A discard closes a READY, FAILED or LOST task without
  running it. Each is one compare-and-set on the row's status and lease number,
  so two retries of one row requeue it once, a discard that races a claim loses
  to it, and a LOST row's missing worker cannot write over its retry. Neither
  touches a RUNNING task. `retry_many(selection)` and `discard_many(selection)`
  take a queryset or a list of ids and make the same move in one conditional
  UPDATE per thousand rows inside one transaction, returning
  `(changed, skipped)`.
- The task table in the Django admin, registered only when
  `django.contrib.admin` is installed: a list with status and queue filters and
  search by id or path, a read-only detail page with every attempt's traceback,
  and **Retry selected tasks** and **Discard selected tasks** actions that
  report how many rows moved and how many were skipped. The actions call
  `retry_many` and `discard_many`, so a select-across of any size is a few
  statements in one transaction. The admin does not add, edit or delete rows.
- `django_ox.bulk.enqueue_many(task, calls)`, the bulk form of `enqueue()`.
  `calls` is a list of `(args, kwargs)` pairs; the rows are written with one
  INSERT per 1,000 inside one transaction and the `TaskResult` list comes back
  in input order. The task is validated and every argument serialised before
  the first write, so a rejected call inserts nothing. Each row is built by the
  same code as `enqueue()`, so workers see no difference.
- `OxTask.Status.DISCARDED`, a sixth value in django-ox's own status column. It
  reads as `FAILED` through `django.tasks` and `is_finished` is true for it.
  `queue_stats()` reports it in a `discarded` column, and `ox_prune` deletes
  discarded rows with successful ones.

### Fixed

- `manage.py check` runs the django-ox checks, `django_ox.E001` to `E005`,
  in a project that imports `django.tasks` nowhere else. Django registers its
  tasks check when that module is first imported, and a project without the
  admin or a task module on its import path reached `check` without it, so
  every django-ox check passed silently. The worker's own startup check was
  unaffected.

### Changed

- **A migration ships with this release.** Run `python manage.py migrate
  django_ox` when you upgrade. It adds the new status choice.
- `QueueStats` has a sixth field, `discarded`, keyword-defaulted like `lost`.
- A queued task can now be discarded and a failed or lost one retried, and
  every attempt can be bounded with `TASK_TIMEOUT`.
- A worker that is already draining, because it is recycling, treats the
  operator's first signal as the drain it is doing rather than as the
  force-exit; the second signal is still the force-exit.

## [0.2.1] - 2026-08-20

### Fixed

- A task that succeeds after its lease was lost drops the reaper's
  lost-lease record from `errors`. That record says the outcome was never
  observed, and the success write is that observation, so nothing reading
  `result.errors` is handed an exception nobody raised. Every earlier
  attempt's traceback stays on the row. A failure resolving the same way
  already dropped it, and the two now agree. A task that is still LOST
  keeps the record: it is the only thing on the row that says why the
  result reads as failed.

### Added

- Built distributions are checked for every migration before release.

## [0.2.0] - 2026-08-20

### Fixed

- A worker whose task had been taken back by the reaper could still write its
  own outcome over the row, so a task that had already finished could be moved
  back to READY and run a second time after its result had been reported. Every
  claim now stamps the row with a lease number, and every finish write carries
  that number in its WHERE clause, so a write from a worker that no longer
  holds the task matches nothing and is dropped instead of applied. No
  completion is signalled for a dropped write.
- A lock that ages out with no attempts left is recorded as LOST rather than
  FAILED. LOST says the worker stopped reporting and the outcome was never
  seen, and nothing more. The `TaskAbandoned` record the reaper leaves in
  `errors` is the lost lease, not a cause of failure.
- Lock timestamps are written and compared using the database server's clock
  rather than each worker's own, so two hosts with drifting clocks no longer
  produce false reclaims. This applies when `USE_TZ` is on. With `USE_TZ` off
  the worker's clock is used instead, because the database's clock does not
  always match what these columns hold: SQLite's is UTC while the columns carry
  naive local time, and reading one against the other would make `ox_prune
  --older-than` treat rows that finished seconds ago as hours old.
- On databases without `SELECT ... FOR UPDATE SKIP LOCKED`, which includes
  SQLite, a claim read its row back in a second statement and could come away
  holding a lease granted to a different worker, if the reaper reclaimed the
  row in the gap between the two. The read is now pinned to the lease the claim
  was granted, so a worker that lost the row inside that gap comes back with
  nothing rather than with someone else's lease.

### Added

- **Lease renewal.** A worker refreshes the lock on the tasks it is running,
  one statement per interval however many are in flight, and keeps doing so
  through a graceful drain. A long task on a healthy worker is no longer
  reclaimed while it is still running. [Editorial note added in 1.4.0:
  releases 0.2.0 to 1.3.1 were still affected by renewal starvation. See the
  1.4.0 entry for the fix.] `LOCK_TIMEOUT` now bounds how long a
  worker may go unresponsive, not how long a task may take. The renewal
  interval is `LOCK_TIMEOUT / 3`, overridable as `renew_interval` when
  embedding `Worker` directly.
- `OxTask.Status.LOST`, a fifth value in django-ox's own status column. It
  reads as `FAILED` through `django.tasks`, which has four statuses and no
  fifth, and `is_finished` is true for it, so callers waiting on a
  result still terminate. The row keeps the distinction: `queue_stats()`
  reports a `lost` column and `ox_prune --include-failed` covers it. If the
  worker holding a LOST task comes back and records a real outcome, that
  outcome replaces LOST; only that one execution can.
- `task_lease_lost` and `lease_renew_failed`, two WARNING log events. Both are
  documented on the Monitoring page.

### Changed

- **A migration ships with this release.** Run `python manage.py migrate
  django_ox` when you upgrade. It adds the `lease_epoch` column and the new
  status choice.
- `task_reclaimed` now reports `status` as `READY` or `LOST`, where it
  previously reported `READY` or `FAILED`.
- `QueueStats` has a fifth field, `lost`. It is keyword-defaulted, so existing
  code that constructs one keeps working.

## [0.1.2] - 2026-08-18

The worker, the public API and the database schema are unchanged. This
release updates the project description that appears on the package page,
and the documentation that ships with it.

### Changed

- README now leads with what the backend removes from a deployment: the
  queue lives in the database the application already runs, so there is no
  broker to provision, secure, upgrade or back up. The transactional
  guarantee follows it rather than opening.

### Added

- Migration guidance now covers moving *away* from django-ox as well as to
  it: which behaviour carries over to a broker-backed backend, which does
  not, and how to keep the option open.
- Worked examples for routing a queue to its own worker, choosing a lock
  timeout for long tasks, overriding a schedule's queue and priority,
  verifying that a schedule is live, and running the worker in containers.
- `context7.json`, so documentation indexers read the project description,
  the supported versions and the setup steps rather than inferring them.

## [0.1.1] - 2026-08-17

The worker, the public API and the database schema are unchanged. This
release updates the packaging metadata and the project description that
appears on the package page.

### Changed

- Packaging metadata now carries a `Documentation` URL, so the
  documentation site is linked directly from the package page.
- README now carries release and CI status badges, a link to the
  documentation site, and a scope statement: what the core covers, what is
  deliberately outside it, and which features belong to the commercial
  tier.

## [0.1.0] - 2026-08-16

Initial release.

### Added

- `OxBackend`, a database-backed backend for Django's Tasks framework
  (`django.tasks`, Django 6.0+). Tasks are stored in the application database;
  no broker required.
- Transactional enqueue: `enqueue()` is a single INSERT on the caller's
  connection, so a task enqueued inside `transaction.atomic()` commits or
  rolls back with the business data.
- `ox_worker` management command: claims tasks with
  `SELECT ... FOR UPDATE SKIP LOCKED` where supported (PostgreSQL, MySQL 8+)
  and an atomic compare-and-set UPDATE elsewhere (including SQLite).
  Configurable via `--backend`, `--queues`, `--concurrency` (thread pool),
  `--interval`, and `--lock-timeout`.
- Retries with exponential backoff (`MAX_ATTEMPTS`, `BACKOFF_INITIAL`,
  `BACKOFF_MAX`), keeping the full traceback of every attempt.
- Reaper: tasks whose worker died are returned to the queue after
  `LOCK_TIMEOUT` and count as a failed attempt.
- Graceful drain: on SIGTERM/SIGINT the worker stops claiming, finishes
  in-flight tasks, then exits; a second signal forces an immediate exit.
- Priorities (-100 to 100, higher first) and deferred tasks (`run_after`),
  with the corresponding `supports_*` flags declared on the backend.
- Result store: `get_result()`, `refresh()`, and the async variants, with
  status, return value, and per-attempt errors readable from the database.
- `ox_prune` management command: batched deletion of finished task rows
  (`--older-than`, `--include-failed`, `--batch-size`, `--dry-run`).
- `django_ox.stats`: read-only queue metrics as plain ORM queries, on
  every supported database: per-queue status counts, backlog depth and
  age, throughput and failure rate over a trailing window, and time
  since the last task claim.
- `ox_health` management command: exits non-zero with a one-line reason
  when the database is unreachable or a `--max-backlog`, `--max-age` or
  `--worker-timeout` threshold is breached; built for cron alerting and
  container probes.
- Structured logging: worker lifecycle events (claim, start, success,
  retry, failure, reclaim, dispatch, shutdown) log to the `django_ox`
  logger with stable extra keys (`event`, `task_id`, `queue`, `attempt`,
  `duration_ms`, ...) for JSON log handlers.
- Recurring tasks: cron schedules declared in the `TASKS` setting
  (`SCHEDULES` option), dispatched by the workers themselves; a unique
  constraint on (schedule, tick) enqueues each due tick once across any
  number of workers. Five-field cron syntax plus `@hourly`-style
  shortcuts; misconfigured schedules fail at startup and in
  `manage.py check`. On recovery after downtime, only the latest missed
  tick fires.
- System check `django_ox.E003`: a schedule name defined on more than
  one backend is rejected, at worker startup and in `manage.py check`,
  because the tick log is keyed by schedule name alone and shared names
  would let the backends suppress each other's ticks.
- Strict cron validation: expressions that can never fire and step values
  larger than a field's range (such as `*/61` in the minute field) are
  rejected at parse time rather than misfiring silently. Schedule
  dispatch holds under clock skew between workers: a tick row dated in
  the future cannot suppress ticks that are due.

### Security

- A stored `task_path` must resolve to a `django.tasks` Task (a function
  registered with `@task`). A row naming any other importable callable is
  rejected as an un-runnable task instead of being executed, so the
  worker never invokes an arbitrary dotted path pulled from the table.
  `SECURITY.md` documents the full trust model, the JSON-only
  serialization, and the guidance to keep secrets out of task arguments.
- An API stability and deprecation policy (`docs/stability.md`) covers
  the public API surface, the pre-1.0 SemVer rule, the deprecation
  window, and the supported Python and Django matrix.

[Unreleased]: https://github.com/oxpull/django-ox/compare/v1.4.0...HEAD
[1.4.0]: https://github.com/oxpull/django-ox/compare/v1.3.1...v1.4.0
[1.3.1]: https://github.com/oxpull/django-ox/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/oxpull/django-ox/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/oxpull/django-ox/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/oxpull/django-ox/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/oxpull/django-ox/compare/v0.4.0...v1.0.0
[0.4.0]: https://github.com/oxpull/django-ox/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/oxpull/django-ox/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/oxpull/django-ox/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/oxpull/django-ox/releases/tag/v0.2.1
[0.2.0]: https://github.com/oxpull/django-ox/releases/tag/v0.2.0
[0.1.2]: https://github.com/oxpull/django-ox/releases/tag/v0.1.2
[0.1.1]: https://github.com/oxpull/django-ox/releases/tag/v0.1.1
[0.1.0]: https://github.com/oxpull/django-ox/releases/tag/v0.1.0
