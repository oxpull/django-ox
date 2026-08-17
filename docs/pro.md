# Pro

Everything documented on this site is free, open source (BSD 3-Clause),
and stays that way. The durable queue, transactional enqueue, retries,
reaper, graceful drain, priorities, deferred tasks, recurring tasks and
pruning are the free tier, permanently.

**django-ox Pro** is a paid tier in development for the problems that
only show up at scale. This page is the roadmap, so you can decide
whether to follow along.

## Planned features

- **Batches**: enqueue N tasks, track them as one unit.
- **Unique tasks**: deduplicate enqueues so a job cannot be queued twice.
- **Rate limiting**: per-queue and per-task throughput caps.
- **Metrics export**: Prometheus and OpenTelemetry.
- **Email support** from the maintainers.

Further out, after launch: workflows and chains (tasks that depend on tasks),
a web dashboard, and encrypted task payloads for teams that need them.

Delivery will be a license-keyed private package index: `pip install`
with your key, no vendoring, no source access ceremony.

## Pricing

Launch pricing will be **$399 per year, per company**, flat, self-serve.
One license covers your whole organization and all environments.
Seven-day money-back.

## Waitlist

If any of the above would earn its keep in your deployment, join the
waitlist and say which feature. That ordering decides what gets built
first.

[Join the Pro waitlist](https://oxpull.github.io/){ .md-button }
