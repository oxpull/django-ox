# Workflows

Workflows connect tasks through dependencies, keeping dependent nodes waiting until they can be released. Workflows are provisional in django-ox 1.9.0. The sections below describe release, completion, retention, and safe shutdown.

See the [feature overview](index.md) for setup, requirements, Django 5.2 behaviour, and the stability policy.

## Behaviour

A workflow is a set of tasks with dependencies. A node runs only after every
node it depends on has succeeded.

```python
from oxpull import workflows

flow = workflows.create("nightly", on_complete=summarize)
flow.add("extract", extract)
flow.add("transform-a", transform, "a", depends_on="extract")
flow.add("transform-b", transform, "b", depends_on="extract")
flow.add("load", load, depends_on=["transform-a", "transform-b"])
flow.seal()

flow.progress()  # WorkflowProgress(total=4, pending=4, blocked=3, ...)
```

A node's backend must declare `WORKFLOWS` and run `OxpullWorker`. The
reconciler must be on a schedule. `add()` refuses a node whose backend does
not declare `WORKFLOWS` or runs another worker class. `create()` and `seal()`
refuse when no schedule runs the reconciler.
Keep workflow tables and their task rows on the same database. See the [database-routing constraints](unique-tasks.md#database-routing-constraints).

```python
TASKS = {
    "default": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "OPTIONS": {
            "WORKFLOWS": True,
            "WORKER_CLASS": "oxpull.worker.OxpullWorker",
            "SCHEDULES": {
                "oxpull-workflow-reconcile": {
                    "task": "oxpull.workflows.reconcile",
                    "cron": "* * * * *",
                },
            },
        },
    }
}
```

`depends_on` names nodes already added to the workflow, so a cycle cannot be
built. A unique task cannot be a node. To make a task unique that workflows
already use, drain its pending nodes first.

## Workflow API

The documented workflow API is provisional:

- `oxpull.workflows.create()`
- `oxpull.workflows.get()`
- `oxpull.workflows.reconcile`
- `oxpull.workflows.retained_rows()`
- `Workflow.add()`
- `Workflow.seal()`
- `Workflow.progress()`
- `WorkflowProgress`

The workflow exceptions are also provisional:

- `oxpull.exceptions.WorkflowSealed`
- `oxpull.exceptions.WorkflowNodeExists`
- `oxpull.exceptions.UnknownWorkflowNode`

They distinguish a sealed workflow, a duplicate node and an unknown node. The `WORKFLOWS` option and all `workflow_*` log events have the same provisional status. Workflow implementation helpers are not public API.

## Waiting and release

- A node with no dependencies is enqueued READY.
- A node with dependencies is inserted in django-ox's WAITING status. No
  worker claims, reaps or prunes a WAITING row.
- An edge is marked satisfied only on proof that its upstream succeeded. The
  proof is the upstream row, or its recorded outcome once the row is pruned.
  SUCCESSFUL is final, so the mark stays true after the upstream row is pruned.
- A node moves from WAITING to READY only when every edge into it is marked.
  The move is a compare-and-set on the row's status and lease epoch.
- The worker that runs a node records its outcome and marks the edges out of
  it in the transaction that writes the task's result. After that commits,
  it releases the nodes those edges held.
- A `run_after` you gave a node stays on its row. The release keeps the
  later of now and that time, so a released node shows a `run_after`.
- The reconciler releases what a worker missed. When release owns its
  transaction, it makes two attempts per chunk of node IDs. If both attempts
  lose a lock conflict, the nodes wait for the reconciler.

  SQLite is supported. Django uses DEFERRED transactions by default.
  `OPTIONS["transaction_mode"]` can change that mode.

  Under DEFERRED transactions, a read-to-write lock conflict can return
  `SQLITE_BUSY` immediately, without waiting for the busy handler.
  After both release attempts lose a lock conflict, release logs
  `workflow_release_deferred` at WARNING level. The reconciler retries
  the deferred nodes.

  Reconciler cadence affects workflow latency. The documented schedule
  runs once a minute. Keep the reconciler scheduled and monitor
  `workflow_release_deferred`.

## The claim still checks

The claim refuses a READY node while an edge into it is not marked and its
upstream row is missing or not SUCCESSFUL. A missing upstream row is never
taken as success. This covers READY rows no release made, such as a django-ox
retry or an UPDATE by hand. What it costs is below.

## Failure, cancellation and completion

- A node does not run while one of its upstreams is failed, lost or
  discarded. In a sealed workflow the reconciler discards that node.
- An upstream whose row is gone counts as dead only when its outcome was
  recorded as dead. With nothing recorded, the nodes downstream of it wait.
- A sealed workflow completes once no node is pending. WAITING, READY and
  RUNNING nodes are pending, as is any status unrecognised by django-ox 1.9.0.
- Completion counts each node by its recorded outcome. A success or a failure
  still counts after `ox_prune` deletes the row. A node whose row went before
  anything recorded it counts as unattributed.
- The reconciler records any outcome a worker did not record, and completes
  the workflow on a later tick.
- `on_complete` and `on_success` are enqueued in the transaction that
  completes the workflow. `on_success` is enqueued only when nothing failed
  and nothing is unattributed.
- A django-ox retry of a node after its workflow completed runs that node.
  The nodes downstream of it stay discarded, and the workflow stays COMPLETE.

## The reconciler's backstop

Each tick pages through open and sealed workflows, one visit each in turn. A
visit marks edges whose upstream succeeded, records outcomes, and releases
waiting nodes. After five seconds, or a quarter of what its task timeout
leaves, a tick starts no further visit or page. Its first page always runs,
and a page that has started runs to its end. The retries of the queue below
come first and are not counted.

On PostgreSQL and MySQL each write waits at most one second for a lock. On
PostgreSQL each of its statements runs at most two seconds. On MySQL that
bound applies to SELECT statements only. The page reads have no time bound. A
row the backstop cannot lock goes to a queue that later ticks retry first. The
next pass reads it again too, so the backstop never gives up on a row.

For SQLite release deferrals and their effect on workflow latency,
see [Waiting and release](#waiting-and-release).

## Retention

`oxpull_prune` keeps batch members. For workflows it follows these rules.

- It considers a workflow only once it is COMPLETE and completed longer ago
  than `--older-than`.
- It stamps the workflow as retired, then deletes nothing while any node is
  pending, such as a node retried after completion.
- It deletes a node, with the edges into it, only when the node's outcome is
  recorded and its task row is SUCCESSFUL, DISCARDED or gone.
- A FAILED or LOST node stays, with the edges into it, while its row exists.
  A retry of it is still checked. It goes once `ox_prune --include-failed`
  deletes the row.
- A node with no recorded outcome is never deleted.
- The workflow row stays.
- A later run passes over a retired workflow whose node rows are all gone. It
  also passes over one that keeps only recorded FAILED or LOST nodes and nodes
  with no recorded outcome. It sends statements for one workflow only while
  it has something to do there.
- A run still reads the row of every workflow that completed before the
  cutoff, and the task row of every node it keeps. Its count of kept rows can
  read the whole node table. Those reads grow with the tables.
- It prints how many node and edge rows it keeps. `workflows.retained_rows()`
  returns the same counts for monitoring.

## Claim cost

Nothing where no backend on the database declares `WORKFLOWS`. Workflows
then add no conjunct to a claim and no statement to a worker. The suite
asserts that against the base worker's statements.

With `WORKFLOWS` on a worker's own backend, its claim probes the edge
table's `oxpull_wf_edge_down_idx` once for every candidate row it judges.
The table is `oxpull_oxpullworkflowedge` and the index is on
`(downstream_task_id, satisfied)`. Every candidate, not only the nodes: a
task that is in no workflow pays the probe, and the probe finds nothing.
That is what the option being the switch costs. A workflow is created at
run time. A cached "there are no workflows" would be a window in which a
node runs out of order. Closing that window with a query per claim pass
would cost more than the probe it replaced.

The reach is the database, not the backend. Wherever any backend on the
database declares `WORKFLOWS`, every `OxpullWorker` there records node
outcomes, because a claim is scoped to queues and not to backends. For a
worker whose own backend does not declare it, that is one column more on the
statement returning the claimed row. Nothing else, for a task that is no
node. Such a worker also looks for the node table every 30 seconds until it
is there, then stops: a cost per worker, not per claim.

The cost is per candidate row, not per blocked node. A blocked node sits in
WAITING, which no claim reads, so a wide fan-in waiting on one root does not
make the claims around it dearer. Measured on PostgreSQL with
`EXPLAIN (ANALYZE)` at 50 and at 450 blocked nodes, and pinned in the suite.

The probe stays a probe. On SQLite and MySQL the claim names the index. On
PostgreSQL the migration sets the column's `n_distinct`, so the planner's
estimate stays small. Statistics gathered while a wide fan-in fills the edge
table do not turn the check into a scan. A plan read on stale statistics is
not comparable to one read on fresh ones, so the suite runs `ANALYZE` before
it measures.

The state is the edge table. It grows with the nodes you add, and
`oxpull_prune` deletes a node's edges with the node, under Retention above.

## Transactions

- A call owns its transaction when the connection is in autocommit and no
  atomic block is open. `add()` then runs its transaction again after a
  deadlock or a serialization failure. `seal()` and completion do so after a
  serialization failure. The cancellation, the record repair and
  `oxpull_prune`'s stamp, deletes and scan row cleanup do so after either.
- Inside your transaction nothing runs again. The error reaches you, and you
  run your transaction again.
- To add to or seal several workflows in one transaction, take them in
  ascending id order. Two transactions that take the same workflows in
  opposite orders can deadlock.
- In that transaction, do not take a locking read or an UPDATE of an existing
  task row.

Use `django_ox.testing.run_tasks(backend="flows")` to run tasks with the workflow backend. Replace `"flows"` with the alias configured for your workflow backend. `run_tasks()` uses that alias's configured worker class. An `OxpullBackend` alias alone does not make the test worker enforce rate limits or release workflow nodes.

## Not supported

- Claiming or running workflow nodes with `run_once()` or `execute()` inside
  a caller's atomic block. Dependents are released by a commit callback of
  the outcome write. Use `run_tasks()` for tests instead.
- Deleting workflow, node or edge rows through the ORM or SQL.
- Mixing releases. Every process using django-ox on the database must run
  django-ox 1.9.0 before any backend declares `WORKFLOWS`. An older release
  without WAITING support cannot read that status.

## Turning workflows off

Two different things go by that name, and only one of them stays available.

**Stop using workflows.** Drain the waiting nodes, then take `WORKFLOWS` out
of `OPTIONS` and the reconciler out of `SCHEDULES`. The tables stay, holding
finished work and nothing running. This is available whenever you want it,
and django-ox's own rollback needs nothing more.

**Drop the tables.** `migrate oxpull` back past
`0005_workflow_edge_upstream_idx` or `0004_workflows` drops the tables that
hold every node, edge and record. Both refuse while any workflow row exists,
and no command deletes a workflow row: `oxpull_prune` deletes nodes and edges
and keeps the row it hangs them on. So this is supported only before the
first workflow is created. After that the row is there for good.

The refusal catches a mistake. It does not make a rollback safe while workers
run.

## Draining the waiting nodes

Take these steps in order, on every database that holds django-ox's tables.

1. Stop creating workflows. Stop every call to `workflows.create()` and
   `add()`, then wait for the requests, jobs and transactions already inside
   one to end.
2. End every open workflow. A node leaves WAITING when it is released or
   discarded. Let the work finish, or close the branches you do not want.
   `django_ox.actions.discard` closes one node's task row, WAITING
   included, and on its next tick the reconciler discards every node below
   it. No call ends a whole workflow at once.
3. Keep the workers and the reconciler running while step 2 runs. Only an
   `OxpullWorker` on a backend that declares `WORKFLOWS` releases a waiting
   node, and only the reconciler discards a branch. Take the option away
   before the rows are gone and nothing is left that can move them.
4. Stop every process that can write a waiting node, and keep it stopped.
   That is anything that calls `add()`. A process already running keeps the
   settings it started with, so changing settings does not stop one.
5. On every alias, not only the one that holds `oxpull`'s tables, run
   `OxTask.objects.using(alias).filter(status="WAITING").count()`. Every
   count must be 0. A count that will not fall means something is still
   adding nodes. Find it, stop it, and go back to step 2.
6. Take `WORKFLOWS` out of `OPTIONS` and the reconciler out of `SCHEDULES`,
   in the settings every process starts with, and restart.

Once no backend declares `WORKFLOWS`, `manage.py check --database <alias>`
reports `oxpull.W007` for any workflow node still WAITING. Name the alias
that holds `oxpull`'s tables. A bare `manage.py check` reports nothing
here, because a check that reads a database runs only against an alias you
name. `ox_health` and `ox_prune` pass their own, so both report it without
the flag. W007 means step 5 was not finished. The hint says to declare
`WORKFLOWS` again, which is how you get back a database that can drain:
declare it, finish step 2, then take it away again. The check reads one
database, so step 5 is what covers the rest.

On MySQL, `oxpull.E009` flags a task-id column on another collation than the
task table's, and a workflow worker refuses to start.

## Deployment order

**Turn it on in this order.** A node with dependencies is inserted in django-ox's WAITING
status. Older django-ox releases without WAITING support
cannot read or release such a row. Every process on the database must run django-ox 1.9.0
before the first waiting node is written. For each database that holds the tables:

1. Upgrade every process using django-ox against that database to django-ox 1.9.0.
   That means workers, web and ASGI processes, enqueue-only services, the reconciler
   and cron jobs.
2. Run `migrate django_ox` and `migrate oxpull` on that database.
3. Check your deploy inventory for processes still running an older release. A process cannot
   see which django-ox version another process runs.
4. Add the workflow settings described above and restart. A running process keeps the settings
   it started with, so nothing changes until it restarts.

For this upgrade, install django-ox 1.9.0 on every enqueuing and claiming process before enabling workflows. This is a consistent-fleet upgrade requirement, not a claim that releases 1.3.0 through 1.8.0 lack WAITING support. WAITING was introduced in django-ox 1.3.0.

Leave step 4 until steps 1 to 3 are done everywhere. Nothing refuses `WORKFLOWS` on the grounds that a process elsewhere is too old, because no process can see the others.

Going back to a django-ox from before WAITING is django-ox's own procedure. Its migration is
the one that refuses while a waiting row exists. Its changelog carries those steps, under
**Rolling back to 1.2 after workflows have run**:
[django-ox changelog](../changelog.md).

## Worker database aliases and pools

For the worker's general connection budget, pool sizing, and renewal fallback requirements, see [database connections and PostgreSQL pooling](../production.md#database-connections-and-postgresql-pooling). The additional alias rules below apply to workflow outcome recording and rate-limit windows.

`oxpull` reads and writes rate-limit windows and records workflow-node outcomes
(node records, edge marks, and release) on the alias returned by
`oxpull_db_alias()`, which must also hold the django_ox tables. When it differs
from `--database`, it needs its own pool and server budget. The worker's
startup warning does not check that alias or available server slots.

### Renewal and watchdog connections

The watchdog's own stuck-attempt READY retries and FAILED records for rows
that are not workflow nodes use its private connection on `--database`. Its
workflow-node final FAILED records also use that connection when the
recording alias matches `--database`. Among watchdog writes, only a workflow
node's final FAILED record on a separate alias uses that alias's ordinary
pooled connection and needs a pooled spare. Task-thread
workflow-node writes (SUCCESSFUL, edge marks, release) and poll-loop
rate-window I/O use the recording alias's ordinary pool; ordinary task
outcome writes use the `--database` pool; neither uses the watchdog's
private connection. The separate-alias recording path has not been tested.

The watchdog uses at most one acquisition sequence per pass: the group of
stuck attempts it records together. This grouping is unrelated to task batches.

Nothing needs setting for renewal or the watchdog. To bound task and poll-loop
connects, which the private-path deadlines do not cover, set
`OPTIONS["connect_timeout"]` on the alias. A positive value below 5 seconds
also shortens private deadlines when it is below their existing budget.

See [pool sizing, connection handling, and deadline limits](../production.md#database-connections-and-postgresql-pooling)
and [monitoring events](../monitoring.md).
On pooled PostgreSQL, connection-acquisition failures no longer use
`lease_renew_failed`; update alerts. Statement failures still use it.

After a PostgreSQL restart, tasks that were running at that moment can run again.

### Rolling upgrades

For workers on django-ox 0.2.0 through 1.3.1, which are affected by the renewal defect,
set pools to at least
`concurrency + 2`, or `concurrency + 3` with task timeouts, before rollout,
or disable their Django pool. Keep that budget until every affected worker has
stopped. The same mitigation applies while an affected worker remains deployed.
