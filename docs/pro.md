# Pro

Everything documented on this site is free, open source (BSD 3-Clause), and
stays that way. The durable queue, transactional enqueue, retries, reaper,
graceful drain, priorities, deferred tasks, recurring tasks and pruning are
the free tier, permanently. Nothing that works today moves behind the paid
tier.

**Oxpull Pro** is a paid add-on for three problems that show up once a queue
is carrying real volume. All three are built and tested. It is not on sale
yet: the purchase and delivery path is still being set up, and the waitlist
below is how to hear when it opens.

## What Pro adds

- **Unique tasks.** Deduplicate at enqueue time, so the same job cannot be
  queued twice. The lock is written in the same transaction as the task, so
  the two commit or roll back together, and a lock whose task died is
  released rather than stranded.
- **Batches.** Enqueue a group, read its progress as a count, and fire a
  callback once every member has settled. Completion is computed by querying
  the task rows rather than by counting signals, so a worker dying mid-task
  cannot strand a batch: the reconciler picks it up on the next tick.
- **Rate limiting.** Cap how often a task starts. A named limit of N
  admissions per period is declared on the task or in settings, and every
  worker shares it through one row in the database. A throttled task stays
  READY: it is not claimed, so it spends no retry attempt and holds no
  lease. With one worker process the cap is exact, at most N per window.
  With W worker processes at most N + W - 1 attempts start in a window,
  and since windows are contiguous, an arbitrary interval of one period
  can carry up to 2(N + W - 1).

All three run on the databases the free tier tests in CI: SQLite, PostgreSQL
and MySQL 8. MariaDB 10.6+ takes the same claim path but is not part of the tested
matrix. Batches have been measured to 100,000 members in a single batch on
SQLite and on PostgreSQL 16.

## What Pro is not

Workflows and chains are on the roadmap, undated. A web dashboard and
encrypted payloads are not in Pro and are not dated. Rate limiting caps how
often a task starts, not how many run at once; concurrency limiting is a
different mechanism and is not in Pro. Metrics stay free: the stats API and
the health command are in the open source package and remain there.

## Delivery

Pro will install from a private package index using credentials issued per
company. There is no licence key and no runtime check. A licence check is one
more thing of ours that can break your production, so we did not build one.
The credential controls access to the index rather than to code you have
already installed, so if it lapses, what is deployed keeps running.

## Pricing

Planned at **$399 per year, per company**, flat. One licence to cover a whole
organisation and every environment, with a seven-day money-back period.

## Waitlist

If Pro would earn its keep in your deployment, join the waitlist and say which
of the three features matters to you. That ordering decides what gets built
after these.

[Join the Pro waitlist](https://oxpull.com/#waitlist){ .md-button }
