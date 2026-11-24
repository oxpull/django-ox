# Autoscaling workers with KEDA

`examples/keda/` in the django-ox repository is a runnable example that scales `ox_worker` pods from zero
on `django_ox_ready_tasks` with KEDA. One script, `check.py`, proves it end to end on a throwaway
[kind](https://kind.sigs.k8s.io/) cluster.

The example covers what queue-driven scaling has to get right:

- **Scale from zero.** Workers start at 0 replicas. KEDA adds them as READY tasks appear, up to 4.
- **A monitoring outage is not an empty queue.** When the scrape fails or Prometheus is down, KEDA runs 2
  fallback replicas.
- **Scale-down finishes the work.** Pods that get SIGTERM mid-task drain first. In the recorded run all 16
  tasks succeeded on their first attempt.
- **A killed worker's task is taken back.** A second trigger keeps 1 replica while any task is RUNNING.
  That replica's reaper returns the task to READY, or marks it LOST when no attempts remain.
- **The endpoint stays private.** NetworkPolicies admit only Prometheus to the metrics endpoint.

## Run it

You need docker, kind and kubectl on your PATH, and Python 3.12 or later with pip. Work from a checkout of
the [django-ox repository](https://github.com/oxpull/django-ox). The example is not in the wheel or the
sdist, and the script builds a django-ox wheel from your checkout.

```bash
python examples/keda/check.py install   # cluster, image, KEDA, the stack
python examples/keda/check.py check     # the end-to-end assertions
python examples/keda/check.py remove    # delete the cluster
```

With no argument, `check.py` runs all three and keeps the cluster if the check fails. Every kubectl call
names the context `kind-ox-keda`, so no other cluster is touched. Worker logs and `report.json` go to
`examples/keda/.run/<timestamp>/`, which is gitignored.

The recorded run took 136 s to install and 544 s to check, about 11.3 minutes for the full cycle.

`install` does the setup, in this order:

1. Creates a one-node kind cluster named `ox-keda`.
2. Builds a django-ox wheel from your checkout and the app image `ox-keda-app:dev` around it, then loads
   the image into kind.
3. Applies KEDA's release manifest for 2.21.1 with `kubectl apply --server-side`.
4. Creates the Secret `ox-keda` with a random database password and Django secret key.
5. Applies the stack, then waits for the migrate Job, the web Deployment, Prometheus and a first successful
   scrape.
6. Applies the ScaledObject last, so KEDA does not count the stack's startup as scaler failures.

`check` then runs seven steps and prints each one:

1. **Network protection.** An unlabelled pod tries the metrics endpoint, Prometheus and the database. Every
   connection has to time out while Prometheus keeps scraping.
2. **Empty queue.** With no rows in the `reports` queue there is no sample, the query reads 0 and the
   workers stay at 0.
3. **Scale up, drain, scale down.** Long tasks scale the workers up. Scale-down sends SIGTERM to pods that
   are mid-task, they drain, and every task has to succeed on its first attempt.
4. **Prune.** Short tasks scale the workers up again. Once they finish, `ox_prune` removes every row, the
   sample disappears and the workers scale down from that state.
5. **Monitoring failure.** A failed scrape, then an unavailable Prometheus, each have to put KEDA into
   fallback at 2 replicas.
6. **Control.** With KEDA's default `ignoreNullValues: "true"`, the same failed scrape reads as 0, with no
   failure counted and no fallback.
7. **Killed worker.** With the second trigger removed, a worker is force-deleted mid-task. KEDA scales to 0
   and the RUNNING row has to outlive its lease untouched. With the trigger restored, a worker has to come
   back, reclaim the task and finish it on attempt 2.

## What's in the directory

| Path | What it holds |
| --- | --- |
| `check.py` | Install, check and remove. |
| `kind.yaml` | The one-node cluster, with the node image pinned by digest. |
| `app/` | Dockerfile, `requirements.txt`, `manage.py` and the `demo` project: one task, `build_report(seconds)`, on the `reports` queue, and a `demo_enqueue` command. |
| `k8s/00-namespace.yaml` | The `ox-keda` namespace. |
| `k8s/05-app-config.yaml` | The settings the migrate Job, web and workers share. |
| `k8s/10-postgres.yaml` | A throwaway PostgreSQL with `max_connections=40`. |
| `k8s/20-migrate.yaml` | The Job that runs `migrate`. |
| `k8s/30-web.yaml` | The Deployment and Service that serve `/ox/metrics`. |
| `k8s/40-worker.yaml` | The `ox_worker` Deployment, starting at 0 replicas. |
| `k8s/50-prometheus.yaml` | Prometheus, scraping every 5 s. |
| `k8s/55-network-policy.yaml` | The NetworkPolicies. |
| `k8s/60-scaledobject.yaml` | The KEDA ScaledObject and its two triggers. |

One image runs the migrate Job, the web Deployment and the worker Deployment. Everything that differs
between them comes from the environment.

## The ScaledObject

This is `k8s/60-scaledobject.yaml` without its opening comment:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: ox-worker
  namespace: ox-keda
spec:
  scaleTargetRef:
    name: ox-worker
  minReplicaCount: 0
  maxReplicaCount: 4
  pollingInterval: 5
  # Short so the check finishes in minutes. In production, start from KEDA's
  # default of 300 seconds and the HPA's default scale-down window.
  cooldownPeriod: 30
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 10
  fallback:
    failureThreshold: 3
    replicas: 2
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.ox-keda.svc.cluster.local:9090
        query: >-
          max(django_ox_ready_tasks{job="django-ox",queue="reports"})
          or (max(up{job="django-ox"}) == 1) * 0
        # Eligible tasks per replica, equal to the worker's --concurrency=2.
        # The gauge leaves out RUNNING tasks, so a higher value would scale
        # workers down while READY work they could take is still waiting.
        threshold: "2"
        activationThreshold: "0"
        ignoreNullValues: "false"
    # One replica while any task is RUNNING. The reaper runs inside workers,
    # so at 0 replicas nothing would reclaim a task whose worker was killed.
    # "> bool 0" turns the RUNNING count into 1 or 0, and threshold "1"
    # makes that one replica, however many tasks are RUNNING.
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.ox-keda.svc.cluster.local:9090
        query: >-
          (max(django_ox_tasks{job="django-ox",queue="reports",status="running"}) > bool 0)
          or (max(up{job="django-ox"}) == 1) * 0
        threshold: "1"
        activationThreshold: "0"
        ignoreNullValues: "false"
```

KEDA's [ScaledObject specification](https://keda.sh/docs/2.21/reference/scaledobject-spec/) documents the
fields. Its [Prometheus scaler](https://keda.sh/docs/2.21/scalers/prometheus/) page documents the trigger
settings.

The two triggers work together. The
[HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) takes the largest replica
count across triggers, and KEDA starts workers from 0 when either trigger is active. The first trigger
sizes the workers to the backlog. The second holds one replica while any task is RUNNING.
[When a worker is killed](#when-a-worker-is-killed) explains why.

### The backlog query and its three readings

```promql
max(django_ox_ready_tasks{job="django-ox",queue="reports"})
or (max(up{job="django-ox"}) == 1) * 0
```

`django_ox_ready_tasks` counts READY rows that are eligible to run now. Scheduled future work and RUNNING
tasks are not in it. The first half takes `max()` rather than `sum()` because every web pod reports the
same database-wide number. There's no `rate()`: the metric is a gauge.

The second half handles a queue with no rows. That queue has no sample, including after pruning removes
its last row. Prometheus marks the series stale on the next scrape, 5 s apart here, and the first half
returns nothing. While the scrape succeeds, the second half returns 0 in its place.

So KEDA has three readings to keep apart:

| Prometheus returns | Meaning | KEDA's reaction |
| --- | --- | --- |
| A sample | The backlog. It is 0 when the queue has rows but none is eligible. | Scales on the value. |
| No sample while `up == 1` | The queue has no rows. | Reads 0 from the second half of the query. |
| Nothing: `up == 0`, or Prometheus is unreachable | Monitoring is down and the backlog is unknown. | Counts a scaler failure. |

`ignoreNullValues: "false"` is what makes the third reading a failure. KEDA's default is `true`, which
treats an empty result as a value. The check's control step ran that default with the web Deployment
scaled to 0. After 35 s KEDA read 0, had counted 0 failures and kept 0 replicas: the outage looked like an
empty queue.

`fallback` sets what KEDA does after repeated failures. Once consecutive failures pass
`failureThreshold: 3`, KEDA reports a value that makes the HPA scale to `replicas: 2`. KEDA 2.21.1 did
this from 0 replicas too. Both triggers read the same Prometheus and the same `up` series, so an outage
fails both. The workers then run although the queue may have no work, so set `replicas` to the count you
want while monitoring is down.

### Threshold equals concurrency

The backlog trigger's `threshold` is the number of READY tasks KEDA plans per replica. Set it to the
worker's `--concurrency`, 2 here. The gauge leaves out RUNNING tasks, so a higher value scales workers
down while READY tasks wait. An earlier run with `threshold: "4"` and concurrency 2 shows it. The HPA went
from 4 replicas to 2 at 26 s after enqueue, with 8 tasks still READY. With `threshold: "2"`, 4 replicas
held until READY reached 0.

`activationThreshold: "0"` starts the first worker as soon as the value is above 0. `pollingInterval: 5`
has KEDA read both triggers every 5 s. Its default is 30 s.

## Shutdown and the grace period

The backlog gauge reads 0 once the last READY task is claimed, while tasks are still RUNNING. The second
trigger then holds 1 replica, and the HPA stops the other pods mid-task. In the recorded run READY reached
0 at 52 s after enqueue. The HPA went to 1 replica at 55 s, and 3 pods got SIGTERM mid-task.

The worker settings in `k8s/40-worker.yaml` make that safe:

```yaml
    spec:
      # SIGTERM starts the drain. The grace period must outlast the longest
      # task, or Kubernetes sends SIGKILL mid-task and leaves it to the reaper.
      terminationGracePeriodSeconds: 120
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
      containers:
        - name: worker
          image: ox-keda-app:dev
          command:
            - python
            - manage.py
            - ox_worker
            - --queues=reports
            - --concurrency=2
            - --heartbeat-file=/run/ox/heartbeat
            # Short so step 7 of the check finishes in minutes. The default
            # is 300 seconds: a killed worker's task stays RUNNING that long
            # before a reaper takes it back.
            - --lock-timeout=60
```

On SIGTERM `ox_worker` stops claiming, [drains its in-flight tasks](production.md#graceful-shutdown) and
exits with code 0. Set `terminationGracePeriodSeconds` longer than your longest task: the example allows
120 s for 40 s tasks.

One of the 3 pods logged these lines, shown here without their timestamp and `INFO django_ox` prefix:

```text
Worker ox-worker-6c688cfc68-8qjpv-1-x1tfLlze received SIGTERM; draining in-flight tasks. Signal again to force exit.
Worker ox-worker-6c688cfc68-8qjpv-1-x1tfLlze draining 2 in-flight task(s)
Task id=b99ecea5-15bd-4fb6-81f6-bc93cce5971a path=demo.tasks.build_report succeeded in 40001ms
Task id=7c1ac2f3-eb66-4331-bfa8-5e66b657c873 path=demo.tasks.build_report succeeded in 40001ms
Worker ox-worker-6c688cfc68-8qjpv-1-x1tfLlze stopped
```

Each of the 3 pods logged `draining 2 in-flight task(s)`, finished both tasks and stopped. The fourth pod
stayed until every task was done. All 16 rows were SUCCESSFUL about 88 s after enqueue, every one on its
first attempt. KEDA scaled to 0 at 118 s.

The startup and liveness probes run `ox_health --heartbeat-file=/run/ox/heartbeat --max-heartbeat-age=60`.
The heartbeat directory is a memory-backed `emptyDir`, private to the pod. Keep it that way: replicas must
not share a heartbeat directory. Run one `ox_worker` process per container and leave the replica count to
KEDA. The Deployment's `replicas: 0` is only the starting value.

### When a worker is killed

A worker can only drain inside the grace period. If the grace period ends first, or the pod is
force-deleted, the worker dies mid-task and the task's row stays RUNNING.

A worker renews the lease on every task it runs. A lease that goes unrenewed for the lock timeout has run
out: 300 s by default, 60 s here. The [reaper](production.md#the-reaper) then takes the task back. The
killed run counts as a failed attempt. If the task has attempts left, it goes back to READY and runs
again. If it has none, the row is marked LOST.

The reaper runs inside every worker. That leaves a gap when the backlog is the only trigger. The gauge
leaves out RUNNING rows, so it reads 0, KEDA scales to 0 and no reaper is left. The task stays RUNNING until
other work starts a worker.

The second trigger closes the gap:

```promql
(max(django_ox_tasks{job="django-ox",queue="reports",status="running"}) > bool 0)
or (max(up{job="django-ox"}) == 1) * 0
```

`django_ox_tasks{status="running"}` is the queue's RUNNING count, and `> bool 0` turns it into 1 or 0.
While the scrape succeeds, the query reads 1 if the queue has a RUNNING task and 0 otherwise. With
`threshold: "1"` that is one replica, however many tasks are RUNNING. A RUNNING row with no workers makes
the trigger active, so KEDA starts one. The second half and `ignoreNullValues: "false"` do the same job as
in the backlog query.

Step 7 of the check shows both sides. With the second trigger removed, it enqueued one 30 s task. Once the
task was RUNNING, it ran `kubectl delete pod --grace-period=0 --force` on the worker. The worker logged the
SIGTERM and `draining 1 in-flight task(s)`, and was killed before the task finished. KEDA scaled to 0.
Twenty seconds after the lease ran out, the row was still RUNNING, with 0 replicas and a KEDA value of 0.

With the trigger restored, a worker came back and its reaper reclaimed the row. The task succeeded on
attempt 2, about 32 s after the restore, and the workers scaled to 0.

## Database connections

Every replica is a database client, so `maxReplicaCount` is also a connection budget. Budget each worker
process at up to `concurrency + 2` server connections: its task threads, the poll loop and a private
lease-renewal connection. That is 4 per worker here and 16 at `maxReplicaCount: 4`, against
`max_connections=40` on the database.

The recorded run peaked at 4 connections per worker and 16 in all, the whole budget. The check fails if a
worker exceeds `concurrency + 2` or the total reaches 40.

Before you raise `maxReplicaCount`, multiply it by `concurrency + 2` and compare the result with your
database's limit. If any task has a timeout, multiply by `concurrency + 3` instead: the timeout watchdog
can open one more connection per worker, and the example sets no timeouts. Leave room for web pods, the
migrate Job and an operator's `psql`. The workers set `CONN_MAX_AGE=600` so each one keeps its connections
between tasks. The Production page's
[Budget server capacity](production.md#budget-server-capacity) section has the numbers for Django's
PostgreSQL pool.

## Network protection

The metrics endpoint has no authentication of its own, so the example lets the network decide who reaches
it. `k8s/55-network-policy.yaml` holds three NetworkPolicies:

- Web pods accept port 8000 only from Prometheus pods.
- Prometheus accepts port 9090 only from the `keda-operator` pod in the `keda` namespace.
- The database accepts port 5432 only from pods labelled `ox-keda/db-client: "true"`: the migrate Job, web
  and workers.

The check runs an unlabelled pod against all three, and every connection timed out in the recorded run.
The same probe with the `db-client` label reached the database, and Prometheus kept scraping. In the run,
kind's default CNI enforced the policies. Repeat the probe on your cluster to confirm that yours does.

The web Deployment here exists only to serve `/ox/metrics`: its URLconf mounts nothing but
`django_ox.urls`. A NetworkPolicy works on ports, not paths. If your main web Deployment serves the site,
run a separate Deployment like this one for the endpoint. Or guard the path in Django, as the
[Monitoring](monitoring.md) page shows.

## The recorded run

The example is documented and tested on one environment, recorded on 2026-11-16 in a full cycle from a
fresh cluster. The machine was an Apple M1 Max Mac (Docker Desktop VM, 6 CPUs). The check passed.

| Component | Version |
| --- | --- |
| kind | v0.33.0 |
| Kubernetes | v1.36.4 (`kindest/node`, pinned by digest) |
| KEDA | 2.21.1 |
| Prometheus | v3.16.0 |
| PostgreSQL | 16.16 (`postgres:16.16-trixie`) |
| Python | 3.12.16 |
| Django | 6.1.3 |
| psycopg | 3.3.9 |
| gunicorn | 27.0.1 |
| django-ox | 1.9.1 as built from the repository's main branch, unreleased changes included |

KEDA 2.21 is tested against Kubernetes v1.34 to v1.36. `check.py` warns when your kind version differs
from the recorded one. The check runs on demand and is not part of django-ox's CI, so rerun it after you
change a version.

What each step observed:

| Step | Observed |
| --- | --- |
| Scale up | 16 tasks of 40 s each brought 4 replicas within 7 s of enqueue. |
| Scale down | READY reached 0 at 52 s. The HPA went to 1 replica at 55 s, and 3 pods got SIGTERM mid-task. |
| Drain | Each of the 3 pods logged `draining 2 in-flight task(s)`, finished both tasks and stopped. The fourth pod stayed until every task was done. All 16 rows were SUCCESSFUL at about 88 s, each on its first attempt. KEDA scaled to 0 at 118 s and counted 0 scaler failures. |
| Prune | After 4 tasks of 2 s, `ox_prune --queue=reports --older-than=0` deleted 20 rows. The sample was gone at the next reading and `up` stayed 1. KEDA read 0 with 0 failures. Replicas read 2, then 1, then 0 after the prune, reaching 0 about 26 s after it. |
| Failed scrape | Scaling the web Deployment to 0 made `up` 0, and both triggers failed. KEDA counted 4 failures on each, went into fallback and ran 2 replicas. After web came back, the workers went back to 0. |
| Prometheus down | Scaling Prometheus to 0 failed both triggers too. KEDA counted 5 failures on one and 4 on the other, went into fallback and ran 2 replicas. After recovery the workers went back to 0. |
| Control | With `ignoreNullValues: "true"` and the web Deployment at 0, after 35 s: `up` 0, KEDA value 0, 0 failures, no fallback, 0 replicas. |
| Killed worker, backlog trigger only | One task of 30 s, and the worker force-deleted once the task was RUNNING. KEDA scaled to 0. Twenty seconds after the lease ran out: the row still RUNNING, 0 replicas, KEDA value 0. |
| Killed worker, both triggers | With the second trigger restored, a worker came back and its reaper reclaimed the row. The task succeeded on attempt 2, about 32 s after the restore. The workers then scaled to 0. |

## Adapting it

Copy `k8s/60-scaledobject.yaml`, with both triggers, and the worker settings from `k8s/40-worker.yaml`
into your own manifests. Then change these:

- **`serverAddress` and `job`.** Point `serverAddress` at your Prometheus in both triggers. Use your scrape
  job's name in every `job` matcher: each query has two.
- **`cooldownPeriod` and the scale-down window.** Both are short here so the check finishes in minutes.
  Delete `cooldownPeriod` to get KEDA's default of 300 s, and delete the `advanced` block to get the HPA's
  default window.
- **`--lock-timeout`.** It is 60 s here so step 7 of the check finishes in minutes. Remove the flag to get
  django-ox's default of 300 s. A killed worker's task stays RUNNING that long before a reaper takes it
  back.
- **`threshold` and `--concurrency`.** Change the backlog trigger's `threshold` and the worker's
  `--concurrency` together, so they stay equal. Leave the second trigger's `threshold` at `"1"`.
- **More queues.** Each query names one queue. For each further queue, add a worker Deployment with its
  own `--queues` and a ScaledObject with that `queue` label in both queries.

Keep both triggers in every ScaledObject that scales workers to 0. With the backlog trigger alone, a killed
worker's task stays RUNNING at 0 replicas until other work arrives.
