# KEDA autoscaling example

This directory scales django-ox workers from zero on `django_ox_ready_tasks` with KEDA. `check.py` builds
a throwaway kind cluster, installs the stack and proves the scaling end to end.

## Requirements

- docker, kind and kubectl on your PATH
- Python 3.12 or later with pip: the script builds a django-ox wheel from this checkout

## Run it

From the repository root:

```bash
python examples/keda/check.py install   # cluster, image, KEDA, the stack
python examples/keda/check.py check     # the end-to-end assertions
python examples/keda/check.py remove    # delete the cluster
```

With no argument, `check.py` runs all three and keeps the cluster if the check fails. Every kubectl call
names the context `kind-ox-keda`, so no other cluster is touched. Worker logs and `report.json` go to
`.run/<timestamp>/` in this directory.

## What to copy

- `k8s/60-scaledobject.yaml`: both triggers, `ignoreNullValues: "false"` and the fallback replicas. The
  first trigger scales on the backlog. The second keeps one replica while any task is RUNNING, so a
  reaper is there to take back a killed worker's task.
- `k8s/40-worker.yaml`: one `ox_worker` per container, the grace period, the heartbeat probes and the
  connection budget.

`cooldownPeriod`, the scale-down window and `--lock-timeout` are short so the check finishes in minutes.
Reset them before production. The docs page says how.

## Documentation

[Autoscaling workers with KEDA](https://oxpull.com/django-ox/keda/) explains the queries, the shutdown
settings, the network policies and the recorded run.
