"""
Install, check and remove the KEDA example on a disposable kind cluster.

    python examples/keda/check.py install   # cluster, image, KEDA, the stack
    python examples/keda/check.py check     # the end-to-end assertions
    python examples/keda/check.py remove    # delete the cluster
    python examples/keda/check.py           # all three; keeps the cluster on failure

Needs docker, kind and kubectl on PATH, and Python 3.12 or later with pip
to build the django-ox wheel from this checkout. Every kubectl call names
the kind context, so no other cluster is touched. Logs and a JSON report
go to examples/keda/.run/<timestamp>/.

The check, in order:

 1. The network refuses an unlabelled pod at the metrics endpoint,
    Prometheus and the database, while Prometheus still scrapes.
 2. With no rows in the queue there is no sample, the query reads 0 and
    the worker Deployment stays at 0.
 3. Long tasks scale the workers up. Once the backlog is claimed, the
    second trigger holds one replica and the HPA stops the other pods
    mid-task. They drain, and every task succeeds on its first attempt.
    The last scale-down happens while the gauge reads 0.
 4. Short tasks scale up again; once they finish, ox_prune removes every
    row, the sample disappears, and scale-down happens from that state.
 5. A failed scrape, then an unavailable Prometheus, each put KEDA into
    fallback at 2 replicas instead of reading an empty queue.
 6. The control: with KEDA's default ignoreNullValues "true", the same
    failed scrape reads as 0, with no failure counted and no fallback.
 7. A worker killed mid-task: with only the backlog trigger, KEDA scales
    to 0 and the RUNNING row outlives its lease untouched. With the second
    trigger restored, a worker comes back and its reaper reclaims the task,
    which succeeds on its second attempt.
"""

from __future__ import annotations

import argparse
import json
import re
import secrets
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
CLUSTER = "ox-keda"
CONTEXT = f"kind-{CLUSTER}"
NS = "ox-keda"
IMAGE = "ox-keda-app:dev"
KIND_VERSION = "v0.33.0"
KEDA_VERSION = "2.21.1"
KEDA_MANIFEST = (
    f"https://github.com/kedacore/keda/releases/download/v{KEDA_VERSION}/"
    f"keda-{KEDA_VERSION}.yaml"
)
SCALEDOBJECT = "60-scaledobject.yaml"
MAX_REPLICAS = 4
FALLBACK_REPLICAS = 2
WORKER_CONCURRENCY = 2
DB_MAX_CONNECTIONS = 40
LONG_TASKS = 16

READY_QUERY = 'max(django_ox_ready_tasks{job="django-ox",queue="reports"})'
UP_QUERY = 'max(up{job="django-ox"})'

_started = time.monotonic()


class CheckFailed(Exception):
    pass


def say(message: str) -> None:
    print(f"[{time.monotonic() - _started:7.1f}s] {message}", flush=True)


def run(
    *cmd: str, check: bool = True, timeout: float = 600, stdin: str | None = None
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            input=stdin,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise CheckFailed(f"{' '.join(cmd)} timed out after {timeout:.0f}s") from exc
    if check and result.returncode != 0:
        raise CheckFailed(
            f"{' '.join(cmd)} exited {result.returncode}:\n"
            f"{result.stdout[-2000:]}{result.stderr[-2000:]}"
        )
    return result


def kubectl(*args: str, check: bool = True, timeout: float = 600) -> str:
    return run(
        "kubectl", "--context", CONTEXT, *args, check=check, timeout=timeout
    ).stdout


def kubectl_ns(*args: str, check: bool = True, timeout: float = 600) -> str:
    return kubectl("-n", NS, *args, check=check, timeout=timeout)


def wait_until[T](
    what: str, probe: Callable[[], T], timeout: float, interval: float = 2.0
) -> T:
    deadline = time.monotonic() + timeout
    error: CheckFailed | None = None
    while True:
        try:
            value = probe()
        except CheckFailed as exc:
            value, error = None, exc
        if value:
            return value
        if time.monotonic() > deadline:
            last = f"; last error: {error}" if error else ""
            raise CheckFailed(
                f"timed out after {timeout:.0f}s waiting for {what}{last}"
            )
        time.sleep(interval)


# --- readings -------------------------------------------------------------


def psql(sql: str) -> str:
    return kubectl_ns(
        "exec", "deploy/ox-db", "--", "psql", "-U", "ox", "-d", "ox", "-tAc", sql
    ).strip()


def task_counts() -> dict[str, int]:
    rows = psql(
        "SELECT status, count(*) FROM django_ox_oxtask "
        "WHERE queue_name = 'reports' GROUP BY status"
    )
    counts: dict[str, int] = {}
    for line in rows.splitlines():
        status, n = line.split("|")
        counts[status] = int(n)
    return counts


def max_attempts() -> int:
    value = psql(
        "SELECT coalesce(max(attempts), 0) FROM django_ox_oxtask "
        "WHERE queue_name = 'reports'"
    )
    return int(value)


def sessions_by_address() -> dict[str, int]:
    rows = psql(
        "SELECT host(client_addr), count(*) FROM pg_stat_activity "
        "WHERE datname = 'ox' AND backend_type = 'client backend' "
        "AND pid <> pg_backend_pid() AND client_addr IS NOT NULL "
        "GROUP BY client_addr"
    )
    result: dict[str, int] = {}
    for line in rows.splitlines():
        address, n = line.split("|")
        result[address] = int(n)
    return result


def prom(query: str) -> list[dict[str, Any]]:
    url = "http://localhost:9090/api/v1/query?" + urllib.parse.urlencode(
        {"query": query}
    )
    body = kubectl_ns("exec", "deploy/prometheus", "--", "wget", "-qO-", url)
    payload = json.loads(body)
    if payload.get("status") != "success":
        raise CheckFailed(f"Prometheus query failed: {payload}")
    return payload["data"]["result"]


def prom_value(query: str) -> float | None:
    """The single value of an instant query, or None when it has no sample."""
    result = prom(query)
    if not result:
        return None
    return float(result[0]["value"][1])


def worker_replicas() -> int:
    value = kubectl_ns("get", "deploy/ox-worker", "-o", "jsonpath={.spec.replicas}")
    return int(value or 0)


def worker_pods() -> list[dict[str, Any]]:
    pods = json.loads(
        kubectl_ns(
            "get", "pods", "-l", "app.kubernetes.io/name=ox-worker", "-o", "json"
        )
    )
    return pods["items"]


def scaledobject() -> dict[str, Any]:
    return json.loads(kubectl_ns("get", "scaledobject/ox-worker", "-o", "json"))


def conditions(obj: dict[str, Any]) -> dict[str, str]:
    return {c["type"]: c["status"] for c in obj.get("status", {}).get("conditions", [])}


def _quantity(text: str) -> float:
    suffixes = {"m": 1e-3, "k": 1e3, "M": 1e6}
    if text and text[-1] in suffixes:
        return float(text[:-1]) * suffixes[text[-1]]
    return float(text)


def keda_failures(obj: dict[str, Any] | None = None) -> int:
    """Consecutive failures KEDA has counted, summed over all triggers."""
    health = (obj or scaledobject()).get("status", {}).get("health", {})
    return sum(entry.get("numberOfFailures", 0) for entry in health.values())


def keda_metric(obj: dict[str, Any] | None = None) -> float | None:
    """
    The backlog trigger's value as KEDA serves it to the HPA, or None while
    the scaler is failing.
    """
    names = (obj or scaledobject()).get("status", {}).get("externalMetricNames") or []
    if not names:
        return None
    selector = urllib.parse.quote("scaledobject.keda.sh/name=ox-worker", safe="")
    path = (
        f"/apis/external.metrics.k8s.io/v1beta1/namespaces/{NS}/{names[0]}"
        f"?labelSelector={selector}"
    )
    result = kubectl("get", "--raw", path, check=False)
    if not result.strip():
        return None
    items = json.loads(result).get("items") or []
    return _quantity(items[0]["value"]) if items else None


# --- worker logs ----------------------------------------------------------


class LogCollector:
    """Follows every worker pod's log into its own file, as pods appear."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.followed: dict[str, subprocess.Popen[bytes]] = {}
        self._files: list[BinaryIO] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=10)
        for process in self.followed.values():
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        for handle in self._files:
            handle.close()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                pods = worker_pods()
            except (CheckFailed, ValueError, OSError):
                pods = []
            for pod in pods:
                name = pod["metadata"]["name"]
                running = any(
                    "running" in s.get("state", {})
                    or "terminated" in s.get("state", {})
                    for s in pod.get("status", {}).get("containerStatuses", [])
                )
                if name in self.followed or not running:
                    continue
                handle = (self.directory / f"{name}.log").open("wb")
                self._files.append(handle)
                self.followed[name] = subprocess.Popen(
                    [
                        "kubectl",
                        "--context",
                        CONTEXT,
                        "-n",
                        NS,
                        "logs",
                        "-f",
                        f"pod/{name}",
                    ],
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                )
            self._stop.wait(1.0)

    def logs(self) -> dict[str, str]:
        return {
            path.stem: path.read_text(errors="replace")
            for path in sorted(self.directory.glob("*.log"))
        }


def drained_mid_task(log: str) -> bool:
    """SIGTERM, then a task succeeding, then a clean stop, in that order."""
    lines = log.splitlines()
    sigterm = next(
        (i for i, line in enumerate(lines) if "received SIGTERM; draining" in line),
        None,
    )
    if sigterm is None:
        return False
    after = lines[sigterm + 1 :]
    succeeded = next(
        (i for i, line in enumerate(after) if "succeeded in" in line), None
    )
    if succeeded is None:
        return False
    return any(re.search(r"Worker \S+ stopped$", line) for line in after[succeeded:])


# --- install / remove -----------------------------------------------------


def require_tools() -> None:
    missing = [tool for tool in ("docker", "kind", "kubectl") if not shutil.which(tool)]
    if missing:
        raise CheckFailed(f"not on PATH: {', '.join(missing)}")
    kind = run("kind", "version").stdout.split()
    if KIND_VERSION not in kind:
        say(f"warning: the recorded run used kind {KIND_VERSION}; found {kind}")


def cluster_exists() -> bool:
    return CLUSTER in run("kind", "get", "clusters").stdout.split()


def build_image() -> None:
    wheel_dir = HERE / "app" / "wheel"
    shutil.rmtree(wheel_dir, ignore_errors=True)
    say("building the django-ox wheel from this checkout")
    run(
        sys.executable,
        "-m",
        "pip",
        "wheel",
        "--no-deps",
        "-q",
        "-w",
        str(wheel_dir),
        str(REPO),
    )
    say(f"building {IMAGE}")
    run("docker", "build", "-q", "-t", IMAGE, str(HERE / "app"), timeout=1200)
    run("kind", "load", "docker-image", IMAGE, "--name", CLUSTER, timeout=600)


def install() -> None:
    require_tools()
    if cluster_exists():
        say(f"reusing the existing kind cluster {CLUSTER}")
    else:
        say(f"creating kind cluster {CLUSTER}")
        run(
            "kind",
            "create",
            "cluster",
            "--config",
            str(HERE / "kind.yaml"),
            "--wait",
            "180s",
            timeout=900,
        )
    build_image()

    say(f"installing KEDA {KEDA_VERSION}")
    kubectl("apply", "--server-side", "-f", KEDA_MANIFEST)
    kubectl(
        "-n",
        "keda",
        "wait",
        "--for=condition=Available",
        "deploy",
        "--all",
        "--timeout=300s",
        timeout=330,
    )

    say("installing the stack")
    kubectl("apply", "-f", str(HERE / "k8s" / "00-namespace.yaml"))
    if not kubectl_ns(
        "get", "secret", "ox-keda", "--ignore-not-found", "-o", "name"
    ).strip():
        run(
            "kubectl",
            "--context",
            CONTEXT,
            "-n",
            NS,
            "create",
            "secret",
            "generic",
            "ox-keda",
            "--from-env-file=/dev/stdin",
            stdin=(
                f"db-password={secrets.token_urlsafe(24)}\n"
                f"django-secret-key={secrets.token_urlsafe(48)}\n"
            ),
        )
    manifests = sorted((HERE / "k8s").glob("*.yaml"))
    for manifest in manifests:
        if manifest.name not in ("00-namespace.yaml", SCALEDOBJECT):
            kubectl("apply", "-f", str(manifest))
    kubectl_ns(
        "wait",
        "--for=condition=Available",
        "deploy/ox-db",
        "--timeout=300s",
        timeout=330,
    )
    kubectl_ns(
        "wait",
        "--for=condition=complete",
        "job/ox-migrate",
        "--timeout=300s",
        timeout=330,
    )
    kubectl_ns("rollout", "status", "deploy/ox-web", "--timeout=300s", timeout=330)
    kubectl_ns(
        "wait",
        "--for=condition=Available",
        "deploy/prometheus",
        "--timeout=300s",
        timeout=330,
    )
    wait_until(
        "Prometheus to scrape the metrics endpoint",
        lambda: prom_value(UP_QUERY) == 1,
        120,
    )

    # Last, so KEDA does not count the stack's startup as scaler failures.
    kubectl("apply", "-f", str(HERE / "k8s" / SCALEDOBJECT))
    wait_until(
        "the ScaledObject to report Ready",
        lambda: conditions(scaledobject()).get("Ready") == "True",
        120,
    )
    say("installed")


def remove() -> None:
    if cluster_exists():
        say(f"deleting kind cluster {CLUSTER}")
        run("kind", "delete", "cluster", "--name", CLUSTER, timeout=300)
    shutil.rmtree(HERE / "app" / "wheel", ignore_errors=True)


# --- the check ------------------------------------------------------------


def sample() -> dict[str, Any]:
    ready = prom(READY_QUERY)
    pods = worker_pods()
    ips = {p["status"].get("podIP") for p in pods if p["status"].get("podIP")}
    sessions = sessions_by_address()
    so = scaledobject()
    return {
        "t": round(time.monotonic() - _started, 1),
        "replicas": worker_replicas(),
        "pods": len(pods),
        "ready_sample": float(ready[0]["value"][1]) if ready else None,
        "up": prom_value(UP_QUERY),
        "keda_value": keda_metric(so),
        "keda_failures": keda_failures(so),
        "counts": task_counts(),
        "worker_sessions_max": max((sessions.get(ip, 0) for ip in ips), default=0),
        "db_sessions": sum(sessions.values()),
    }


def enqueue(count: int, seconds: float) -> None:
    kubectl_ns(
        "exec",
        "deploy/ox-web",
        "--",
        "python",
        "manage.py",
        "demo_enqueue",
        f"--count={count}",
        f"--seconds={seconds}",
    )


def probe_refusals(report: dict[str, Any]) -> None:
    say("1. network protection: an unlabelled pod tries each service")
    script = (
        "import json, socket\n"
        "out = {}\n"
        "for name, port in (('ox-web', 8000), ('prometheus', 9090), ('ox-db', 5432)):\n"
        "    try:\n"
        "        socket.create_connection((name, port), timeout=3).close()\n"
        "        out[name] = 'open'\n"
        "    except OSError as exc:\n"
        "        out[name] = type(exc).__name__\n"
        "print(json.dumps(out))\n"
    )
    results = {}
    for label, pod in (
        ("", "ox-probe"),
        ("ox-keda/db-client=true", "ox-probe-db-client"),
    ):
        kubectl_ns("delete", "pod", pod, "--ignore-not-found", "--wait=true")
        args = ["run", pod, f"--image={IMAGE}", "--restart=Never"]
        if label:
            args.append(f"--labels={label}")
        kubectl_ns(*args, "--command", "--", "python", "-c", script)
        kubectl_ns(
            "wait",
            "--for=jsonpath={.status.phase}=Succeeded",
            f"pod/{pod}",
            "--timeout=90s",
            timeout=120,
        )
        results[pod] = json.loads(kubectl_ns("logs", f"pod/{pod}").strip())
        kubectl_ns("delete", "pod", pod, "--wait=false")
    report["network"] = results
    say(f"   {results}")
    if any(v == "open" for v in results["ox-probe"].values()):
        raise CheckFailed(f"an unlabelled pod reached a protected service: {results}")
    # The positive control: the same probe with the client label reaches the
    # database, so the refusals above come from the policies.
    if results["ox-probe-db-client"]["ox-db"] != "open":
        raise CheckFailed(f"a labelled client could not reach the database: {results}")
    if prom_value(UP_QUERY) != 1:
        raise CheckFailed("Prometheus can no longer scrape the metrics endpoint")


def check_absent_baseline(report: dict[str, Any]) -> None:
    say("2. empty queue: no sample, the query reads 0, no workers")
    if task_counts():
        raise CheckFailed("the reports queue is not empty; run against a fresh install")
    if prom(READY_QUERY):
        raise CheckFailed("django_ox_ready_tasks has a reports sample with no rows")
    wait_until("KEDA to read 0", lambda: keda_metric() == 0, 60)
    so = conditions(scaledobject())
    report["baseline"] = {
        "keda_value": keda_metric(),
        "keda_failures": keda_failures(),
        "conditions": so,
        "replicas": worker_replicas(),
    }
    if (
        worker_replicas() != 0
        or so.get("Fallback") == "True"
        or report["baseline"]["keda_failures"]
    ):
        raise CheckFailed(f"expected 0 replicas and no fallback: {report['baseline']}")


def watch_until_scaled_to_zero(
    what: str, timeout: float, samples: list[dict[str, Any]]
) -> None:
    deadline = time.monotonic() + timeout
    while True:
        reading = sample()
        samples.append(reading)
        if reading["replicas"] == 0 and len(samples) > 1:
            break
        if time.monotonic() > deadline:
            raise CheckFailed(f"timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(2)
    # Scaled to 0 means SIGTERM has been sent, not that the drain is over.
    # The grace period in k8s/40-worker.yaml bounds the wait.
    wait_until("the last worker pod to drain and exit", lambda: not worker_pods(), 150)


def check_long_tasks(report: dict[str, Any], collector: LogCollector) -> None:
    count, seconds = LONG_TASKS, 40
    say(f"3. enqueue {count} tasks of {seconds}s: scale up, drain, scale down")
    enqueue(count, seconds)
    samples: list[dict[str, Any]] = [sample()]
    wait_until("the first worker replica", lambda: worker_replicas() > 0, 90)
    say("   scaled up from 0")
    peak = wait_until(
        f"{MAX_REPLICAS} worker replicas",
        lambda: (r := worker_replicas()) == MAX_REPLICAS and r,
        180,
    )
    say(f"   reached {peak} replicas")
    watch_until_scaled_to_zero("scale-down to 0 after the long tasks", 420, samples)
    counts = task_counts()
    say(f"   scaled to 0; rows {counts}, max attempts {max_attempts()}")
    report["long_tasks"] = {"samples": samples, "counts": counts, "peak_replicas": peak}
    if counts != {"SUCCESSFUL": count}:
        raise CheckFailed(f"expected {count} SUCCESSFUL rows, found {counts}")
    if any(s["keda_failures"] for s in samples):
        raise CheckFailed("KEDA counted a scaler failure while monitoring was healthy")
    if max_attempts() != 1:
        raise CheckFailed("a task needed more than one attempt: a drain was cut short")
    last = samples[-2]
    if last["ready_sample"] != 0 or last["up"] != 1:
        raise CheckFailed(f"scale-down did not happen while the gauge read 0: {last}")
    say("   scale-down happened while the gauge read 0")

    time.sleep(3)  # let the followers flush the last pod's final lines
    logs = collector.logs()
    drained = [name for name, text in logs.items() if drained_mid_task(text)]
    report["long_tasks"]["pods_seen"] = sorted(logs)
    report["long_tasks"]["drained_mid_task"] = drained
    say(f"   {len(drained)} of {len(logs)} worker pods drained a task after SIGTERM")
    if not drained:
        raise CheckFailed("no worker log shows a task finishing after SIGTERM")


def check_absent_scale_down(report: dict[str, Any]) -> None:
    count, seconds = 4, 2
    say(f"4. enqueue {count} tasks of {seconds}s, prune the queue, scale down")
    enqueue(count, seconds)
    wait_until("a worker replica", lambda: worker_replicas() > 0, 90)
    wait_until(
        "the new tasks to finish",
        lambda: task_counts() == {"SUCCESSFUL": LONG_TASKS + count},
        120,
        interval=1,
    )
    if worker_replicas() == 0:
        raise CheckFailed("scaled down before the prune; raise cooldownPeriod")
    output = kubectl_ns(
        "exec",
        "deploy/ox-web",
        "--",
        "python",
        "manage.py",
        "ox_prune",
        "--queue=reports",
        "--older-than=0",
    )
    say(f"   ox_prune: {output.strip()}")
    if task_counts():
        raise CheckFailed(f"rows remain after the prune: {task_counts()}")
    wait_until("the sample to disappear", lambda: not prom(READY_QUERY), 30, interval=1)
    if worker_replicas() == 0:
        raise CheckFailed(
            "scaled down before the sample disappeared; raise cooldownPeriod"
        )
    samples: list[dict[str, Any]] = [sample()]
    watch_until_scaled_to_zero("scale-down to 0 from the absent state", 120, samples)
    report["absent"] = {"samples": samples}
    absent = [
        s
        for s in samples
        if s["ready_sample"] is not None or s["up"] != 1 or s["keda_failures"]
    ]
    if absent:
        raise CheckFailed(f"a sample reappeared or KEDA saw a failure: {absent}")
    say("   scale-down happened with no sample and a healthy scrape")


def check_fallback(report: dict[str, Any], deployment: str, what: str) -> None:
    kubectl_ns("scale", f"deploy/{deployment}", "--replicas=0")
    kubectl_ns(
        "wait",
        "--for=delete",
        "pod",
        "-l",
        f"app.kubernetes.io/name={deployment}",
        "--timeout=120s",
        timeout=150,
        check=False,
    )
    if deployment == "ox-web":
        wait_until(
            "the failed scrape to show",
            lambda: prom_value(UP_QUERY) == 0,
            30,
            interval=1,
        )

    def in_fallback() -> dict[str, Any] | None:
        obj = scaledobject()
        return obj if conditions(obj).get("Fallback") == "True" else None

    obj = wait_until(f"fallback after {what}", in_fallback, 120)
    replicas = wait_until(
        f"{FALLBACK_REPLICAS} fallback replicas",
        lambda: (r := worker_replicas()) == FALLBACK_REPLICAS and r,
        120,
    )
    health = obj.get("status", {}).get("health", {})
    report.setdefault("fallback", {})[what] = {"replicas": replicas, "health": health}
    say(f"   {what}: fallback active, {replicas} replicas, health {health}")
    kubectl_ns("scale", f"deploy/{deployment}", "--replicas=1")
    kubectl_ns(
        "rollout", "status", f"deploy/{deployment}", "--timeout=180s", timeout=210
    )
    wait_until("the scrape to recover", lambda: prom_value(UP_QUERY) == 1, 60)
    wait_until(
        "fallback to clear",
        lambda: conditions(scaledobject()).get("Fallback") != "True",
        90,
    )
    wait_until("scale-down to 0 after recovery", lambda: worker_replicas() == 0, 150)
    say("   recovered and scaled back to 0")


def check_default_null_handling(report: dict[str, Any]) -> None:
    """
    The control for step 5: with KEDA's default ignoreNullValues "true",
    the same failed scrape reads as an empty queue and nothing reports it.
    """
    say('6. control: with ignoreNullValues "true" a failed scrape reads as 0')
    patch = json.dumps({"spec": {"triggers": _triggers_with(ignoreNullValues="true")}})
    kubectl_ns("patch", "scaledobject/ox-worker", "--type=merge", "-p", patch)
    try:
        kubectl_ns("scale", "deploy/ox-web", "--replicas=0")
        wait_until("the failed scrape", lambda: prom_value(UP_QUERY) == 0, 60)
        # More than six polls: twice the failure threshold.
        time.sleep(35)
        reading = {
            "up": prom_value(UP_QUERY),
            "keda_value": keda_metric(),
            "keda_failures": keda_failures(),
            "fallback": conditions(scaledobject()).get("Fallback"),
            "replicas": worker_replicas(),
        }
        report["control_default_null_handling"] = reading
        say(f"   {reading}")
        if reading != {
            "up": 0.0,
            "keda_value": 0.0,
            "keda_failures": 0,
            "fallback": "False",
            "replicas": 0,
        }:
            raise CheckFailed(f"the control did not read the failure as 0: {reading}")
    finally:
        kubectl("apply", "-f", str(HERE / "k8s" / SCALEDOBJECT))
        kubectl_ns("scale", "deploy/ox-web", "--replicas=1")
        kubectl_ns("rollout", "status", "deploy/ox-web", "--timeout=180s", timeout=210)
    wait_until("the scrape to recover", lambda: prom_value(UP_QUERY) == 1, 60)
    say("   restored the example's ScaledObject")


def _triggers_with(**metadata: str) -> list[dict[str, Any]]:
    triggers = scaledobject()["spec"]["triggers"]
    for trigger in triggers:
        trigger["metadata"] = {**trigger["metadata"], **metadata}
    return triggers


def lease_still_held() -> bool:
    """The one RUNNING row's lease has not run out yet."""
    value = psql(
        "SELECT bool_and(lease_expires_at > now()) "
        "FROM django_ox_oxtask WHERE status = 'RUNNING'"
    )
    return value == "t"


def lease_expired_20s_ago() -> bool:
    """The one RUNNING row's lease ran out at least 20 seconds ago."""
    value = psql(
        "SELECT bool_and(lease_expires_at < now() - interval '20 seconds') "
        "FROM django_ox_oxtask WHERE status = 'RUNNING'"
    )
    return value == "t"


def check_orphaned_task(report: dict[str, Any]) -> None:
    """
    A worker killed outright leaves its task RUNNING until the reaper, which
    runs inside workers, takes it back. Without the second trigger KEDA
    scales to 0 and the task waits until other work starts a worker. With
    it, a worker comes back for the RUNNING row and its reaper reclaims the
    task.
    """
    say("7. a worker killed mid-task: stranded with one trigger, reclaimed with two")
    backlog_only = scaledobject()["spec"]["triggers"][:1]
    patch = json.dumps({"spec": {"triggers": backlog_only}})
    kubectl_ns("patch", "scaledobject/ox-worker", "--type=merge", "-p", patch)
    try:
        enqueue(1, 30)
        wait_until("the task to start", lambda: task_counts() == {"RUNNING": 1}, 90, 1)
        busy = [p["metadata"]["name"] for p in worker_pods()]
        kubectl_ns("delete", "pod", *busy, "--grace-period=0", "--force")
        say(f"   force-deleted {busy} while the task ran")
        wait_until(
            "scale-down to 0 with the task RUNNING",
            lambda: worker_replicas() == 0 and not worker_pods(),
            150,
        )
        lease_outlived_workers = lease_still_held()
        # Twenty seconds past the lease, so the row is a reaper's to take.
        wait_until("the lease to run out", lease_expired_20s_ago, 180)
        stranded = {
            "lease_outlived_workers": lease_outlived_workers,
            "counts": task_counts(),
            "replicas": worker_replicas(),
            "keda_value": keda_metric(),
        }
        say(f"   lease expired, nothing reclaimed it: {stranded}")
        if stranded != {
            "lease_outlived_workers": True,
            "counts": {"RUNNING": 1},
            "replicas": 0,
            "keda_value": 0,
        }:
            raise CheckFailed(f"expected a stranded RUNNING row at 0: {stranded}")
    finally:
        kubectl("apply", "-f", str(HERE / "k8s" / SCALEDOBJECT))
    say("   restored the second trigger")
    wait_until("a worker for the RUNNING row", lambda: worker_replicas() >= 1, 90)
    wait_until(
        "the reaper to reclaim it and the task to succeed",
        lambda: task_counts() == {"SUCCESSFUL": 1},
        240,
    )
    attempts = max_attempts()
    samples: list[dict[str, Any]] = [sample()]
    watch_until_scaled_to_zero("scale-down to 0 after the reclaimed task", 150, samples)
    report["orphaned_task"] = {
        "stranded": stranded,
        "attempts": attempts,
        "samples": samples,
    }
    say(f"   reclaimed and succeeded on attempt {attempts}; scaled back to 0")
    if attempts != 2:
        raise CheckFailed(f"expected the reclaimed task on attempt 2, got {attempts}")


def versions() -> dict[str, str]:
    def image(namespace: str, deployment: str) -> str:
        return kubectl(
            "-n",
            namespace,
            "get",
            f"deploy/{deployment}",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].image}",
        )

    app = kubectl_ns(
        "exec",
        "deploy/ox-web",
        "--",
        "python",
        "-c",
        "import django, django_ox; print(django.__version__, django_ox.__version__)",
    ).split()
    server = json.loads(kubectl("version", "-o", "json"))["serverVersion"]["gitVersion"]
    return {
        "kind": run("kind", "version").stdout.strip(),
        "kubernetes": server,
        "keda": image("keda", "keda-operator"),
        "prometheus": image(NS, "prometheus"),
        "postgres": image(NS, "ox-db"),
        "django": app[0],
        "django_ox": app[1],
    }


def check(run_dir: Path) -> None:
    report: dict[str, Any] = {"started": datetime.now(UTC).isoformat()}
    report["versions"] = versions()
    say(f"versions: {report['versions']}")
    collector = LogCollector(run_dir)
    collector.start()
    try:
        probe_refusals(report)
        check_absent_baseline(report)
        check_long_tasks(report, collector)
        check_absent_scale_down(report)
        say("5. monitoring failure is not an empty queue")
        check_fallback(report, "ox-web", "failed scrape")
        check_fallback(report, "prometheus", "Prometheus unavailable")
        check_default_null_handling(report)
        check_orphaned_task(report)
        peak = max(
            s["worker_sessions_max"]
            for key in ("long_tasks", "absent")
            for s in report[key]["samples"]
        )
        total = max(
            s["db_sessions"]
            for key in ("long_tasks", "absent")
            for s in report[key]["samples"]
        )
        report["connections"] = {"per_worker_peak": peak, "all_clients_peak": total}
        say(f"   database sessions: {peak} per worker at most, {total} in all")
        if peak > WORKER_CONCURRENCY + 2 or total >= DB_MAX_CONNECTIONS:
            raise CheckFailed(f"connection budget exceeded: {report['connections']}")
        report["result"] = "pass"
    except CheckFailed as exc:
        report["result"] = f"fail: {exc}"
        raise
    finally:
        collector.stop()
        report["finished"] = datetime.now(UTC).isoformat()
        (run_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        say(f"report and worker logs: {run_dir}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "action",
        nargs="?",
        default="all",
        choices=("all", "install", "check", "remove"),
    )
    args = parser.parse_args()
    run_dir = HERE / ".run" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    try:
        if args.action == "remove":
            remove()
            return 0
        if args.action in ("all", "install"):
            install()
        if args.action in ("all", "check"):
            run_dir.mkdir(parents=True)
            check(run_dir)
            say("PASS")
        if args.action == "all":
            remove()
    except CheckFailed as exc:
        say(f"FAIL: {exc}")
        if args.action == "all":
            say(
                f"cluster kept for inspection; remove it with: python {__file__} remove"
            )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
