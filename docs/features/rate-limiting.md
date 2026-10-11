# Rate limiting

Rate limiting controls task claims through `OxpullWorker`. Its guarantee depends on the workers applying the limit and recording claims; the bound and its assumptions are described below.

See the [feature overview](index.md) for setup, requirements, and compatibility.

Rate limiting coordinates workers through database state. Every decision is a
compare-and-set resolved by a rowcount, never a check-then-act: the
window roll is an UPDATE conditioned on the window id, so exactly one
worker among any number opens a window and the losers report zero rows
and keep the view they read. The increment that counts an admission is
deliberately unconditional, because by the time it runs the attempt is
already spent and a statement that could refuse would burn it. What stops
a worker taking a second token is not that statement but the next
refresh, which every worker is required to make before its next limited
claim.

The window's clock is the database's, on every database and under any
`USE_TZ` setting. `window_started_at` is written with `Now()` and is only
ever compared against `Now()` in the same statement family, so there is
no second writer for it to agree with and no per-host clock in the
decision. A worker's own clock decides only when to look, which can waste
a statement but can never admit a task. A database clock that steps
backwards delays the next roll by the size of the step and under-admits;
one that steps forwards rolls the window early and releases its capacity
ahead of schedule, by at most one window per step. Neither compounds: the
window re-anchors to database time on the next roll.

## Behaviour

A rate limit caps how often a task starts. Give it a name, a capacity,
and the tasks or queues it covers:

```python
TASKS = {
    "default": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["default", "sms", "reports"],
        "OPTIONS": {
            "WORKER_CLASS": "oxpull.worker.OxpullWorker",
            "RATE_LIMITS": {
                # Both tasks draw on one budget of 100 per minute.
                "stripe": {
                    "limit": 100,
                    "per": 60,
                    "tasks": [
                        "myapp.billing.charge_card",
                        "myapp.billing.refund_card",
                    ],
                },
                "reports": {
                    "limit": 10,
                    "per": 3600,
                    "tasks": ["myapp.reports.build_monthly"],
                },
                # A whole queue, whatever runs on it.
                "sms": {"limit": 500, "per": 60, "queues": ["sms"]},
            },
        },
    }
}
```

A limit is declared in settings. A worker imports no task modules, so a
limit written next to its task would be enforced only when something else
happened to import that module first. Declared here, it is visible to
every worker and to `manage.py check`.

`RATE_LIMITS`, `UNIQUE_TASKS` and `@unique` take effect only with
`oxpull.backend.OxpullBackend`. On an alias still using
`django_ox.backend.OxBackend`, nothing reads these settings:
`manage.py check` is clean, and nothing is throttled or deduplicated.

For aliases using `OxpullBackend`, `WORKER_CLASS` is required: it makes
`ox_worker` run the worker that applies the limits. Configuring limits
without it is a hard system check error (`oxpull.E006`). `per` is whole
seconds, or a `timedelta` of at least one second. A limit that covers
nothing, names a queue the backend does not accept, or lists a `tasks`
path that is not a task is `oxpull.E005`. These checks run only for
aliases whose `BACKEND` is `oxpull.backend.OxpullBackend`.

## Deployment guarantees

Rate limits are applied in `OxpullWorker.claim_one()`, not enforced by a database constraint. The base `Worker` claims tasks without applying the limit or recording a count.

Deploy django-ox 1.9.0 and use `OxpullWorker` in every process that claims limited tasks before relying on the guarantee. `manage.py check` reads only its own process's settings and cannot verify which worker other processes run.

## The guarantee

> For a limit of N admissions per period P, with W `ox_worker` processes
> claiming from the queues that carry the limit's tasks, the number of
> task attempts started in any one window is at most N + W - 1 + U.
> U is the number of admissions whose count did not land: one for each
> count that raised, and one for each count that the window rolled out
> from under five times in a row. Each one is logged at warning as
> `rate_limit_uncounted`; alert on that event, and U is the number of
> times it fired in the window. Add one for each worker process that
> dies between claiming a limited task and recording it. With a single
> worker process, no such death and every count landed, the limiter is
> exact: at most N per window.
>
> A window is one generation of the limit's shared counter row: it opens
> when a worker rolls the row and lasts P. It is not every interval of
> length P. Windows are contiguous, so an arbitrary interval of length P
> that spans a boundary can carry up to 2(N + W - 1 + U). The limiter
> therefore bounds a long-run average of (N + W - 1 + U)/P and an
> instantaneous burst of 2(N + W - 1 + U).
>
> A count that cannot be written is logged and the attempt runs. Three
> admissions in a row that the counter did not record close the limit
> for that worker process, whether each increment raised or ran and
> matched no row. A closed limit admits nothing until a write lands,
> which the next refresh probes for. The run and the closed state are
> per process, and only an increment that lands ends the run, so a
> counter that fails on every other admission closes after three of
> them rather than admitting through the fault.

The W - 1 is structural. The budget has to be read before the claim,
because the claim UPDATE spends a retry attempt and a lease epoch and
neither can be given back; between reading "this limit has room" and the
claim landing, another worker can take the last token. Closing that gap
would mean burning attempts on tasks that never ran.

W is yours to know, not ours. One `ox_worker` process is one claimer, and
under `--processes N` each child is a claimer. Size against the bound
rather than against the capacity. For a ceiling of T attempts in one
window, configure **`limit = T - (W - 1)`**. For a ceiling of T across
any interval of one period, including one that straddles a boundary,
halve it: **`limit = T // 2 - (W - 1)`**. Both assume U is zero, which
holds while `rate_limit_uncounted` is not firing; every time it fires,
the window it fired in can carry one attempt more than the ceiling.
Scaling workers up loosens a limit that is left alone, and each worker
that dies mid-claim costs one more admission.

Semantics, all deliberate:

- **It limits attempt starts, not completions and not concurrency.** Ten
  per minute on a task that runs for ten minutes will have a hundred
  attempts running at once. "At most K running" is a concurrency limit,
  which is a different mechanism.
- **Retries count.** A reaped or failed attempt that goes back to READY
  consumes another admission when it is claimed again. That is correct
  for what limits usually protect: a retry is another call to the third
  party.
- **A throttled task stays READY.** It is not claimed, not locked, and it
  spends no attempt and no lease epoch. It is indistinguishable from any
  other queued task, and nothing in the reaper, the lease or the timeouts
  interacts with it.
- **A throttled task does not hold up anything else.** The exclusion is
  applied inside the claim's candidate select on all three claim paths,
  ahead of its ordering and its limit, so a worker whose limit is
  exhausted still claims other tasks and other queues on the same pass.
- **Windows are fixed and anchored at first use**, so a P-length interval
  straddling a boundary can carry twice the capacity. If what you are
  protecting counts per fixed minute, size N at half your true ceiling or
  accept the doubling.
- **A task may belong to several limits.** It is admitted only when every
  one of them has room, and admitting it increments every one.
- **`limit: 0` blocks the limit entirely.** It is a supported pause
  switch, and it holds unique keys open, holds batches open, and drives
  the ready-age metric up for as long as it is set.
- **Capacity is settings, not data.** A change takes effect for a worker
  when that worker restarts. During a rolling change of `limit`,
  admissions are bounded by the larger of the two capacities. During a
  rolling change of `per`, the bound is set by the shorter period until
  the last worker on it is gone: the window row carries no period, so a
  worker on the shorter one rolls the shared window as soon as its own
  period allows, and every worker then counts against the fresh window.
  Change `per` in its own deploy, and expect the shorter period to hold
  until that deploy finishes.
- **A limit's name spans every backend.** Its window row is keyed on the
  name alone, so two backends that use the same name share a window and
  count each other's admissions. Give a limit meant for one backend a name
  only that backend uses. A name with a different `per` on two backends is
  warning `oxpull.W005`, and that limit is not enforced as configured.
  Each backend rolls the shared window at its own period. Each roll
  restarts the count, so a backend on a longer period can admit more than
  its limit in that period. A different `limit` with the same `per` is
  warning `oxpull.W004`. Each backend then stops when the shared count
  reaches its own `limit`. That shared budget can be deliberate.
- **A limit's name is matched by the database's collation.** The name is
  the primary key of the limit's window row, so on MySQL's case-folding
  collations renaming `Stripe` to `stripe` is not a rename: the worker
  finds the row it already has and carries on counting on it. On
  PostgreSQL and SQLite the spelling is the identity, and a renamed limit
  starts a fresh window. Within one backend, names that differ only by case
  are refused at `check` as `oxpull.E005`. Anything else the database folds
  (`Café` and `cafe` on MySQL) is refused at the worker's first refresh,
  before it claims, naming both. Across backends, names that differ only by
  case or accents are warning `oxpull.W006`. MySQL's default collation
  counts them on one window, and PostgreSQL and SQLite count them on two. On
  one backend, `oxpull.W006` also covers names that differ only by accents,
  or by case and accents, such as `stripe` and `stripé`. On MySQL's default
  collation, the worker refuses such a pair too. When `check` compares
  names, it drops accents from Latin and Greek letters only. It does not
  catch other folds MySQL's default collation makes, such as `ø` into `o`.
  Spell a name the same way on every backend.
- **Renaming a limited task detaches its queued rows.** The limit matches
  on the task path stored in the row, so rows enqueued under the old path
  claim unthrottled. List both paths under the limit for one release.
- **A counter that cannot be written closes its limit.** One failed count
  is logged and the claimed task is returned for execution: the claim is
  already committed by then, and losing a count is better than losing
  the task. The same holds for any database failure after the claim, the
  re-read after a window rolled under the count included, and for that
  re-read finding the limit's row gone: the loss is logged as
  `rate_limit_uncounted` and the claimed task is returned for execution.
  Before the claim a failure ends the pass with nothing spent, and a
  missing row propagates as `ImproperlyConfigured`. Three unrecorded
  admissions in a row under one limit stop it admitting altogether,
  because a counter that never advances is not a limit, and every
  refresh would read room that is not there. An increment that raised
  and one that ran but matched no row both count; only an increment that
  lands ends the run. The refresh probes the row and the limit reopens
  when a write lands, without a worker restart.

## Claim cost

Nothing when it is unused. With no limits configured, and no backend on the
database declaring `WORKFLOWS`, the claim emits django-ox's own SQL,
statement for statement. The suite asserts that by capturing both.
See [workflow claim cost](workflows.md#claim-cost) for the work a `WORKFLOWS` backend adds to every claim on that database.
With limits configured, a task that no limit covers costs one
refresh SELECT per cooldown interval
(`RATE_REFRESH_INTERVAL`, 0.25 s by default), shared by every claim in
it. A limited task costs two extra statements: that refresh and one
increment. A window roll costs a third. One roll lands per window per
limit across the whole fleet; a worker that reads the expired window
before the winner's write commits spends one more UPDATE, which matches
no row.

The state is one row per limit name in `oxpull_oxpullratewindow`,
forever. The table does not grow with traffic and needs no pruning.

## Throttled work reads as backlog, because it is

`django_ox.stats.oldest_ready_age` and the Prometheus ready-age gauge
climb for as long as work is throttled. A queue-age alert tuned before
rate limiting was introduced will fire. That is the limiter working, not
a stuck queue: alert on the age of queues that carry no limited tasks, or
raise the threshold above the longest period you configured.

Two interactions matter when you combine features. A throttled `@unique`
task keeps its key, so throttling extends the dedup window; that is what
"unique while pending" means, and releasing the key would let duplicates
stack up behind the throttle. And a batch does not complete past a
throttled member, so a limit sets the floor on how long the batch runs.
On one worker, a 1,000-member batch under a limit of 10 per minute fills
100 windows, and the hundredth opens 99 minutes after the first. The
batch completes once its last member finishes, plus one reconciler tick.
Size batches against limits.

## Database aliases and pools

Rate-limit window reads and writes use the alias returned by `oxpull_db_alias()`. See [worker database aliases and pools](workflows.md#worker-database-aliases-and-pools) for the recording alias's pool requirements and the limits of the worker's startup warning.

Use the same database alias for task rows and rate-limit windows: `OxpullWorker` reads the window on that alias. See the [database-routing constraints](unique-tasks.md#database-routing-constraints).
