# Pro

django-ox is free and open source (BSD 3-Clause). The durable queue,
transactional enqueue, retries, reaper, graceful drain, priorities, deferred
tasks, recurring tasks and pruning are in the free package.

**Oxpull Pro** is a paid add-on for four problems that show up once a queue
is carrying real volume. All four are built and tested. <https://oxpull.com/> has the details.

Pro requires Django 6.0 or later. It builds on `django.tasks`, which is part
of Django core from 6.0. django-ox itself also runs on Django 5.2 LTS through
the `django-tasks` backport; Pro does not.

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
  Complete is a reading of the task rows, not proof that every member has
  stopped running.
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
  task attempts started in any one window is at most N + W - 1 + U. U is
  the number of admissions whose count did not land. Each one logs
  `rate_limit_uncounted`, so U is the number of times that event fired.
  Add one for each worker process that dies between claiming a limited
  task and recording it. With a single worker process, no such death and
  every count landed, the limiter is exact: at most N per window. Windows
  are contiguous, so an arbitrary interval of length P that spans a
  boundary can carry up to 2(N + W - 1 + U). A count that cannot be
  written is logged and the attempt runs; three uncounted admissions in a
  row on one limit close it until a write lands.
- **Workflows.** Declare a set of tasks with dependencies. A node runs only
  after every node it depends on has succeeded. The backend declares
  `OPTIONS["WORKFLOWS"]`, sets `OPTIONS["WORKER_CLASS"]` to
  `oxpull.worker.OxpullWorker`, and schedules `oxpull.workflows.reconcile`.
  Without that schedule, `create()` and `seal()` refuse, and
  `manage.py check` reports `oxpull.W003`. A reconciler exposed to
  schedule rows on a queue no backend accepts reports `oxpull.E008`
  instead. Workflows need Oxpull Pro 1.3.0 or later.

Each Pro release pins django-ox exactly: `oxpull==1.4.0` pins
`django-ox==1.4.0`.

Pro runs on the databases the free tier tests in CI: SQLite,
PostgreSQL and MySQL 8. MariaDB 10.6+ takes the same claim path but is not
part of the tested matrix. Batches have been measured to 1,000,000 members
in a single batch on all three, with every count checked against the task
rows rather than against what the API reports about itself.

Sealing a batch that wide from inside a caller's transaction takes about
34 ms on SQLite, 0.7 s on PostgreSQL and 28 s on MySQL. That path counts
with a row lock per member wherever the database offers one, and a
million InnoDB locks in one transaction is where the MySQL figure comes
from. Called outside a transaction, `seal()` counts with a single
`COUNT(*)` instead. Timing those two statements alone against a
million-member batch with fresh statistics, the locking read took 12 s on
MySQL and 0.55 s on PostgreSQL, and the `COUNT(*)` 0.7 s on MySQL and
0.03 s on PostgreSQL. The whole `seal()` call on the same harness and the
same batch, outside a transaction, took 0.74 s on MySQL and 0.03 s on
PostgreSQL.
Reconciling the same batch is 4.9 s on SQLite, 0.4 s on PostgreSQL and
7.3 s on MySQL, against a reconciler that runs on a one-minute tick.

Measured on an arm64 macOS host, with SQLite on a local file in WAL mode
at `synchronous=NORMAL`, and stock PostgreSQL 16 and MySQL 8 images in
Docker on localhost, each width against an empty database.

## What Pro is not

Chains are on the roadmap, undated. Rate limiting caps how
often a task starts, not how many run at once; concurrency limiting is a
different mechanism and is not in Pro. Metrics stay free: the stats API and
the health command are in the open source package and remain there.

## Delivery

Pro installs from a private package index using credentials issued per
company. There is no licence key and no runtime check. A licence check would
put a validation step in the path of code that has to keep running, and it
does nothing for a company that has already paid. Nothing in the package
phones home: no network call to Oxpull, and no telemetry. The credential controls
access to the index rather than to code you have already installed, so if it
lapses, what is deployed keeps running.

## Pricing

**$399 per year, per company**, excluding VAT where it applies. One licence
covers a whole organisation and every environment. The term is 12 months and
renews for successive 12-month terms unless you cancel. Cancel by writing to
support@oxpull.com. Cancellation takes effect at the end of the period you
have paid for. The full terms are stated at purchase.

## Ordering

Order at [oxpull.com](https://oxpull.com/#order) with your company name, a
contact name, an email address, the billing address and, if you have one, a
VAT ID for the invoice. The invoice goes out within two business days. When it
is paid, the credentials for the private package index arrive by email, and
install is `pip install`. Questions to support@oxpull.com.

[Order Pro](https://oxpull.com/#order){ .md-button }
