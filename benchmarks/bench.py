#!/usr/bin/env python
"""
Benchmark harness: django-ox vs django-tasks-db on PostgreSQL 16.

Run with the project venv python (psycopg and both backends installed):

    ../../.venv/bin/python bench.py            # from this directory
    ../../.venv/bin/python bench.py --runs 3   # default

See README.md in this directory for methodology and limitations.

The orchestrator (no --role flag) never imports Django. Every measurement
runs in a fresh subprocess (a "role") so no backend benefits from a warm
process. Raw results are written to results-raw-<date>.json; the published
results file is authored from that JSON by a human.
"""

import argparse
import datetime
import json
import os
import platform
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent

PG = {
    "host": "127.0.0.1",
    "port": 54330,
    "user": "postgres",
    "password": "ox",
}
CONTAINER = "ox-bench"
IMAGE = "postgres:16"
DOCKER = ["docker", "--context", "desktop-linux"]

ENQUEUE_COUNT = 2000
LATENCY_COUNT = 500
WARMUP_COUNT = 20  # identical for both backends; rows deleted before timing
E2E_COUNT = 2000
E2E_TIMEOUT = 900  # seconds before an end-to-end run is declared failed
POLL_INTERVAL = 0.05


def apply_smoke_mode() -> None:
    """Tiny counts to validate the pipeline. Never for published numbers."""
    global ENQUEUE_COUNT, LATENCY_COUNT, E2E_COUNT, E2E_TIMEOUT
    ENQUEUE_COUNT = 30
    LATENCY_COUNT = 30
    E2E_COUNT = 20
    E2E_TIMEOUT = 120

BACKENDS = {
    "ox": {
        "settings": "benchsite.settings_ox",
        "database": "bench_ox",
        "table": "django_ox_oxtask",
        "tasks_module": "benchsite.tasks_ox",
    },
    "tasksdb": {
        "settings": "benchsite.settings_tasksdb",
        "database": "bench_tasksdb",
        "table": "django_tasks_database_dbtaskresult",
        "tasks_module": "benchsite.tasks_tasksdb",
    },
}


# --------------------------------------------------------------------------
# Roles: run inside a fresh subprocess, one measurement each.
# --------------------------------------------------------------------------


def role_setup(backend_name: str):
    os.environ["DJANGO_SETTINGS_MODULE"] = BACKENDS[backend_name]["settings"]
    sys.path.insert(0, str(BENCH_DIR))
    import django

    django.setup()
    import importlib

    module = importlib.import_module(BACKENDS[backend_name]["tasks_module"])
    return module.noop


def role_enqueue_throughput(backend_name: str, count: int) -> None:
    """Time `count` sequential enqueues in autocommit mode."""
    noop = role_setup(backend_name)
    from django.db import connection

    for _ in range(WARMUP_COUNT):
        noop.enqueue()
    with connection.cursor() as cursor:
        cursor.execute(f'DELETE FROM "{BACKENDS[backend_name]["table"]}"')

    start = time.perf_counter()
    for _ in range(count):
        noop.enqueue()
    elapsed = time.perf_counter() - start
    print(json.dumps({"seconds": elapsed, "tasks_per_sec": count / elapsed}))


def role_enqueue_latency(backend_name: str, count: int) -> None:
    """
    Latency of a single enqueue() call made inside transaction.atomic().

    Each iteration opens its own atomic block; the timed window covers only
    the enqueue() call (the INSERT on the open transaction), not COMMIT.
    """
    noop = role_setup(backend_name)
    from django.db import connection, transaction

    for _ in range(WARMUP_COUNT):
        noop.enqueue()
    with connection.cursor() as cursor:
        cursor.execute(f'DELETE FROM "{BACKENDS[backend_name]["table"]}"')

    latencies_ms = []
    for _ in range(count):
        with transaction.atomic():
            start = time.perf_counter()
            noop.enqueue()
            latencies_ms.append((time.perf_counter() - start) * 1000)
    print(json.dumps({"latencies_ms": latencies_ms}))


def role_enqueue_batch(backend_name: str, count: int) -> None:
    """Enqueue `count` tasks, untimed (workload for the end-to-end metric)."""
    noop = role_setup(backend_name)
    for _ in range(count):
        noop.enqueue()
    print(json.dumps({"enqueued": count}))


# --------------------------------------------------------------------------
# Orchestrator helpers (no Django; psycopg only).
# --------------------------------------------------------------------------


def pg_connect(dbname: str):
    import psycopg

    return psycopg.connect(
        host=PG["host"],
        port=PG["port"],
        user=PG["user"],
        password=PG["password"],
        dbname=dbname,
        autocommit=True,
    )


def ensure_docker() -> None:
    probe = subprocess.run(
        [*DOCKER, "ps"], capture_output=True, text=True
    )
    if probe.returncode == 0:
        return
    print("Docker daemon not responding; launching Docker Desktop...")
    subprocess.run(["open", "-a", "Docker"], check=True)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if subprocess.run([*DOCKER, "ps"], capture_output=True).returncode == 0:
            return
        time.sleep(2)
    sys.exit("Docker daemon did not come up within 180s")


def ensure_container() -> None:
    inspect = subprocess.run(
        [*DOCKER, "inspect", "-f", "{{.State.Running}}", CONTAINER],
        capture_output=True,
        text=True,
    )
    if inspect.returncode != 0:
        print(f"Starting container {CONTAINER} ({IMAGE}, port {PG['port']})...")
        subprocess.run(
            [
                *DOCKER, "run", "-d", "--name", CONTAINER,
                "-e", f"POSTGRES_PASSWORD={PG['password']}",
                "-p", f"{PG['port']}:5432",
                IMAGE,
            ],
            check=True,
            capture_output=True,
        )
    elif inspect.stdout.strip() != "true":
        subprocess.run([*DOCKER, "start", CONTAINER], check=True, capture_output=True)

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            with pg_connect("postgres"):
                return
        except Exception:
            time.sleep(1)
    sys.exit("PostgreSQL did not accept connections within 90s")


def ensure_databases() -> None:
    with pg_connect("postgres") as conn:
        for cfg in BACKENDS.values():
            exists = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (cfg["database"],)
            ).fetchone()
            if not exists:
                conn.execute(f'CREATE DATABASE "{cfg["database"]}"')


def subprocess_env(backend_name: str) -> dict:
    env = os.environ.copy()
    env["DJANGO_SETTINGS_MODULE"] = BACKENDS[backend_name]["settings"]
    env["PYTHONPATH"] = str(BENCH_DIR) + os.pathsep + env.get("PYTHONPATH", "")
    return env


def migrate(backend_name: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "django", "migrate", "-v", "0"],
        env=subprocess_env(backend_name),
        check=True,
    )


def truncate(backend_name: str) -> None:
    cfg = BACKENDS[backend_name]
    with pg_connect(cfg["database"]) as conn:
        conn.execute(f'TRUNCATE "{cfg["table"]}"')


def count_status(backend_name: str, status: str) -> int:
    cfg = BACKENDS[backend_name]
    with pg_connect(cfg["database"]) as conn:
        row = conn.execute(
            f'SELECT count(*) FROM "{cfg["table"]}" WHERE status = %s', (status,)
        ).fetchone()
        return row[0]


def run_role(backend_name: str, role: str, count: int, log_name: str) -> dict:
    """Run a measurement role in a fresh subprocess; return its JSON result."""
    log_path = BENCH_DIR / "logs" / f"{log_name}.log"
    result = subprocess.run(
        [
            sys.executable, str(BENCH_DIR / "bench.py"),
            "--role", role, "--backend", backend_name, "--count", str(count),
        ],
        env=subprocess_env(backend_name),
        capture_output=True,
        text=True,
    )
    log_path.write_text(
        f"# exit={result.returncode}\n# stdout\n{result.stdout}\n# stderr\n{result.stderr}\n"
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"role {role} for {backend_name} failed (see {log_path}):\n{result.stderr[-2000:]}"
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


def worker_commands(
    backend_name: str, concurrency: int, extra_args: list[str] | None = None
) -> list[list[str]]:
    """
    Worker invocation per backend.

    ox: one process, --concurrency N (thread pool).
    tasksdb: db_worker has no concurrency option, so N worker processes.
      --no-startup-delay disables its random <=1s startup sleep (a
      thundering-herd nicety, exposed as a flag by the package itself).
      Disabling it can only improve tasksdb's numbers. Everything else
      at defaults.
    """
    extra = extra_args or []
    if backend_name == "ox":
        return [
            [
                sys.executable, "-m", "django", "ox_worker",
                "--concurrency", str(concurrency), *extra,
            ]
        ]
    return [
        [sys.executable, "-m", "django", "db_worker", "--no-startup-delay", *extra]
        for _ in range(concurrency)
    ]


def run_e2e(
    backend_name: str,
    concurrency: int,
    log_name: str,
    extra_args: list[str] | None = None,
) -> dict:
    """
    Pre-load E2E_COUNT tasks, then measure from just before the worker
    process(es) are spawned until all rows are SUCCESSFUL. Includes worker
    process startup and Django init (identical procedure for both backends).
    """
    truncate(backend_name)
    run_role(backend_name, "enqueue-batch", E2E_COUNT, f"{log_name}-producer")

    ready = count_status(backend_name, "READY")
    if ready != E2E_COUNT:
        raise RuntimeError(f"expected {E2E_COUNT} READY rows, found {ready}")

    env = subprocess_env(backend_name)
    logs = []
    procs = []
    start = time.perf_counter()
    for i, cmd in enumerate(worker_commands(backend_name, concurrency, extra_args)):
        log_file = open(BENCH_DIR / "logs" / f"{log_name}-worker{i}.log", "w")
        logs.append(log_file)
        procs.append(
            subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        )

    outcome = {"backend": backend_name, "concurrency": concurrency}
    try:
        deadline = time.monotonic() + E2E_TIMEOUT
        while True:
            successful = count_status(backend_name, "SUCCESSFUL")
            if successful >= E2E_COUNT:
                outcome["seconds"] = time.perf_counter() - start
                outcome["tasks_per_sec"] = E2E_COUNT / outcome["seconds"]
                break
            failed = count_status(backend_name, "FAILED")
            if failed:
                outcome["error"] = f"{failed} tasks FAILED"
                break
            dead = [p for p in procs if p.poll() is not None]
            if dead:
                outcome["error"] = (
                    f"{len(dead)} worker process(es) exited early "
                    f"(codes {[p.returncode for p in dead]})"
                )
                break
            if time.monotonic() > deadline:
                outcome["successful_at_timeout"] = successful
                outcome["timeout_seconds"] = E2E_TIMEOUT
                outcome["error"] = (
                    f"timeout after {E2E_TIMEOUT}s with {successful} SUCCESSFUL"
                )
                break
            time.sleep(POLL_INTERVAL)
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
        for proc in procs:
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for log_file in logs:
            log_file.close()
    return outcome


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile on the sorted sample."""
    ordered = sorted(values)
    rank = max(1, int(round(fraction * len(ordered) + 0.5)) - 1)
    return ordered[min(rank, len(ordered) - 1)]


def collect_environment() -> dict:
    def sysctl(name: str) -> str:
        out = subprocess.run(
            ["/usr/sbin/sysctl", "-n", name], capture_output=True, text=True
        )
        return out.stdout.strip()

    from importlib.metadata import version

    with pg_connect("postgres") as conn:
        pg_version = conn.execute("SHOW server_version").fetchone()[0]

    return {
        "cpu": sysctl("machdep.cpu.brand_string"),
        "memory_bytes": int(sysctl("hw.memsize") or 0),
        "logical_cpus": sysctl("hw.ncpu"),
        "macos": platform.mac_ver()[0],
        "python": platform.python_version(),
        "postgres_server": pg_version,
        "packages": {
            name: version(name)
            for name in ("Django", "django-ox", "django-tasks-db", "django-tasks", "psycopg")
        },
    }


def orchestrate(runs: int, smoke: bool = False, resume: bool = False) -> None:
    ensure_docker()
    ensure_container()
    ensure_databases()
    (BENCH_DIR / "logs").mkdir(exist_ok=True)
    for name in BACKENDS:
        print(f"Migrating {name}...")
        migrate(name)

    results = {
        "date": datetime.date.today().isoformat(),
        "environment": collect_environment(),
        "parameters": {
            "enqueue_count": ENQUEUE_COUNT,
            "latency_count": LATENCY_COUNT,
            "warmup_count": WARMUP_COUNT,
            "e2e_count": E2E_COUNT,
            "runs": runs,
            "order": "interleaved per run: ox then tasksdb",
        },
        "enqueue_throughput": [],
        "enqueue_latency": [],
        "e2e": [],
    }
    suffix = "-SMOKE" if smoke else ""
    out_path = BENCH_DIR / f"results-raw-{results['date']}{suffix}.json"
    if resume and out_path.exists():
        prior = json.loads(out_path.read_text())
        for key in ("enqueue_throughput", "enqueue_latency", "e2e", "e2e_diagnostic"):
            if key in prior:
                results[key] = prior[key]
        print(f"Resuming: loaded existing measurements from {out_path}")

    def have(section: str, **keys) -> bool:
        return any(
            all(entry.get(k) == v for k, v in keys.items())
            for entry in results.get(section, [])
        )

    def checkpoint():
        out_path.write_text(json.dumps(results, indent=2))

    for run in range(1, runs + 1):
        for name in BACKENDS:
            if not have("enqueue_throughput", backend=name, run=run):
                print(f"[run {run}] {name}: enqueue throughput ({ENQUEUE_COUNT})...")
                truncate(name)
                r = run_role(
                    name, "enqueue-throughput", ENQUEUE_COUNT,
                    f"{name}-throughput-run{run}",
                )
                r.update(backend=name, run=run)
                results["enqueue_throughput"].append(r)
                print(f"    {r['tasks_per_sec']:.0f} tasks/sec ({r['seconds']:.2f}s)")
                checkpoint()

            if not have("enqueue_latency", backend=name, run=run):
                print(f"[run {run}] {name}: enqueue latency ({LATENCY_COUNT})...")
                truncate(name)
                r = run_role(
                    name, "enqueue-latency", LATENCY_COUNT, f"{name}-latency-run{run}"
                )
                lat = r.pop("latencies_ms")
                r.update(
                    backend=name,
                    run=run,
                    p50_ms=percentile(lat, 0.50),
                    p95_ms=percentile(lat, 0.95),
                    mean_ms=statistics.fmean(lat),
                    min_ms=min(lat),
                    max_ms=max(lat),
                    latencies_ms=lat,
                )
                results["enqueue_latency"].append(r)
                print(f"    p50 {r['p50_ms']:.2f}ms  p95 {r['p95_ms']:.2f}ms")
                checkpoint()

            for concurrency in (1, 4):
                if have("e2e", backend=name, concurrency=concurrency, run=run):
                    continue
                print(
                    f"[run {run}] {name}: end-to-end {E2E_COUNT} tasks, "
                    f"concurrency {concurrency}..."
                )
                r = run_e2e(name, concurrency, f"{name}-e2e-c{concurrency}-run{run}")
                r["run"] = run
                results["e2e"].append(r)
                if "error" in r:
                    print(f"    ERROR: {r['error']}")
                else:
                    print(
                        f"    {r['seconds']:.1f}s ({r['tasks_per_sec']:.0f} tasks/sec)"
                    )
                checkpoint()

            if name == "ox" and not have("e2e_diagnostic", run=run):
                # Diagnostic row, NOT part of the defaults comparison: the
                # default worker loop sleeps its full poll interval whenever
                # the single executor slot is momentarily busy, which caps
                # concurrency-1 throughput near 1/interval for fast tasks.
                # This run lowers --interval to 0.1 purely to confirm that
                # diagnosis; the defaults row above is the honest headline.
                print(
                    f"[run {run}] {name}: DIAGNOSTIC end-to-end {E2E_COUNT} tasks, "
                    "concurrency 1, --interval 0.1 (non-default)..."
                )
                r = run_e2e(
                    name, 1, f"{name}-e2e-c1-int01-run{run}",
                    extra_args=["--interval", "0.1"],
                )
                r["run"] = run
                r["label"] = "diagnostic: non-default --interval 0.1"
                results.setdefault("e2e_diagnostic", []).append(r)
                if "error" in r:
                    print(f"    ERROR: {r['error']}")
                else:
                    print(
                        f"    {r['seconds']:.1f}s ({r['tasks_per_sec']:.0f} tasks/sec)"
                    )
                checkpoint()

    print(f"\nRaw results written to {out_path}")
    print("Container left running; remove with:")
    print(f"  docker --context desktop-linux rm -f {CONTAINER}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", choices=["enqueue-throughput", "enqueue-latency", "enqueue-batch"])
    parser.add_argument("--backend", choices=list(BACKENDS))
    parser.add_argument("--count", type=int, default=0)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Pipeline validation with tiny counts; never for published numbers.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Load an existing results-raw file for today and skip completed cells.",
    )
    args = parser.parse_args()
    if args.smoke:
        apply_smoke_mode()

    if args.role:
        if not args.backend:
            sys.exit("--role requires --backend")
        if args.role == "enqueue-throughput":
            role_enqueue_throughput(args.backend, args.count or ENQUEUE_COUNT)
        elif args.role == "enqueue-latency":
            role_enqueue_latency(args.backend, args.count or LATENCY_COUNT)
        elif args.role == "enqueue-batch":
            role_enqueue_batch(args.backend, args.count or E2E_COUNT)
        return

    orchestrate(args.runs, smoke=args.smoke, resume=args.resume)


if __name__ == "__main__":
    main()
