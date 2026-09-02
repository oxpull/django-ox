# Pro

django-ox is free, open source (BSD 3-Clause), and stays that way. The
durable queue, transactional enqueue, retries, reaper, graceful drain,
priorities, deferred tasks, recurring tasks and pruning are the free tier,
permanently. Nothing that works today moves behind the paid tier.

**Oxpull Pro** is a paid add-on for three problems that show up once a queue
is carrying real volume. All three are built and tested. It is not on sale
yet. The waitlist below is how to hear when it opens.

## What Pro adds

- **Unique tasks.** Deduplicate at enqueue time: while a task with the same
  key is READY or RUNNING, enqueueing it again returns the pending task's
  result rather than inserting a second row. The lock is written in the
  same transaction as the task, so the two commit or roll back together,
  and a lock whose task settled or was pruned is released rather than
  stranded.
  A bulk enqueue is one INSERT that the deduplicating path never sees, so
  `enqueue_many()` raises `InvalidTask` for a unique task and names it.
- **Batches.** Enqueue a group, read its progress as a count, and fire a
  callback once every member has settled. Completion is computed by querying
  the task rows rather than by counting signals, so a worker dying mid-task
  cannot strand a batch: the reconciler picks it up on the next tick.
  Batches take one setting: `oxpull.batches.reconcile` on a one-minute cron
  in `OPTIONS["SCHEDULES"]`, run by the django-ox cron you already have.
- **Rate limiting.** Cap how often a task starts. A named limit of N
  admissions per period is declared in `OPTIONS["RATE_LIMITS"]`, and every
  worker shares it through one row in the database. A throttled task stays
  READY: it is not claimed, so it spends no retry attempt and holds no
  lease. Set `OPTIONS["WORKER_CLASS"]` to `oxpull.worker.OxpullWorker`, the
  worker that applies the limits. `manage.py check` reports `oxpull.E006`
  when a limit is configured without it.

  For a limit of N admissions per period P, with W `ox_worker` processes
  claiming from the queues that carry the limit's tasks, the number of
  task attempts started in any one window is at most N + W - 1. Add one
  for each worker process that dies between claiming a limited task and
  recording it. With a single worker process and no such death the
  limiter is exact: at most N per window. Windows are contiguous, so an
  arbitrary interval of length P that spans a boundary can carry up to
  2(N + W - 1). A count that cannot be written is logged and the
  attempt runs; three consecutive failed counts on one limit close it
  until a write lands.

All three run on the databases the free tier tests in CI: SQLite,
PostgreSQL and MySQL 8. MariaDB 10.6+ takes the same claim path but is not
part of the tested matrix. Batches have been measured to 1,000,000 members
in a single batch on all three, with every count checked against the task
rows rather than against what the API reports about itself.

Sealing a batch that wide is cheap on SQLite and PostgreSQL and expensive
on MySQL: about 34 ms, 0.7 s and 28 s respectively. Sealing takes a row
lock per member on any database that offers one, and a million InnoDB
locks in a single transaction is what that costs. Reconciling the same
batch is 4.9 s on SQLite, 0.4 s on PostgreSQL and 7.3 s on MySQL, against
a reconciler that runs on a one-minute tick.

Measured on an arm64 macOS host, with SQLite on a local file in WAL mode
at `synchronous=NORMAL`, and stock PostgreSQL 16 and MySQL 8 images in
Docker on localhost, each width against an empty database.

## What Pro is not

Workflows and chains are on the roadmap, undated. A web dashboard and
encrypted payloads are not in Pro and are not dated. Rate limiting caps how
often a task starts, not how many run at once; concurrency limiting is a
different mechanism and is not in Pro. Metrics stay free: the stats API and
the health command are in the open source package and remain there.

## Delivery

Pro installs from a private package index using credentials issued per
company. There is no licence key and no runtime check. A licence check would
put a validation step in the path of code that has to keep running, and it
does nothing for a company that has already paid. Nothing in the package
phones home: no network call to us, and no telemetry. The credential controls
access to the index rather than to code you have already installed, so if it
lapses, what is deployed keeps running.

## Pricing

Planned at **$399 per year, per company**, flat. One licence to cover a whole
organisation and every environment, with a seven-day money-back period. The
term is 12 months and renews for successive 12-month terms unless you cancel.
Cancel by writing to support@oxpull.com. Cancellation takes effect at the end
of the period you have paid for.

## Waitlist

If Pro would earn its keep in your deployment, join the waitlist and say which
of the three features matters to you. That ordering decides what gets built
after these.

[Join the Pro waitlist](https://oxpull.com/#waitlist){ .md-button }
