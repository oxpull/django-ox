#!/usr/bin/env python
"""
Benchmark harness: django-ox vs django-tasks-db on PostgreSQL 16.

Run with the project venv python (psycopg and both backends installed):

    ../../.venv/bin/python bench.py                   # default matrix, 5 runs
    ../../.venv/bin/python bench.py --smoke --runs 1  # pipeline check, tiny N

See README.md in this directory for what each cell measures, its boundaries
and the disclosures.

The orchestrator (no --role flag) imports Django only to record the
environment. Every measurement runs in a fresh subprocess (a "role") so no
backend benefits from a warm process. Raw results are written to
results-raw-<date>.json; the published results page is written from that
JSON.
"""

import argparse
import datetime
import json
import os
import platform
import shlex
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

import django_ox

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

ARMS = ("ox", "tasksdb")
BULK_ARMS = ("ox-many", "ox-loop", "tasksdb-loop")
CELLS = (
    "throughput", "latency", "e2e", "diagnostic", "legacy", "probe", "bulk", "retry",
)

ENQUEUE_COUNT = 2000
LATENCY_COUNT = 500
WARMUP_COUNT = 20  # identical for both backends; rows deleted before timing
BULK_COUNT = 10_000  # payloads per bulk enqueue arm, one transaction
RETRY_COUNT = 20  # tasks that raise once in the retry behaviour row
RETRY_WINDOW = 60.0  # seconds the retry row is observed before it is closed
LEDGER_TABLE = "bench_retry_ledger"  # see benchsite/retry_ledger.py
DEFAULT_DEPTHS = (2000, 20000)  # READY rows preloaded for the end-to-end cells
TOPOLOGIES = (1, 4)  # worker processes per arm in the end-to-end cells
POLL_INTERVAL = 0.05  # observer poll at depth 2,000; see poll_interval()
QUIET_LOAD = 2.0  # --require-quiet refuses at or above this one-minute load
STABILITY_THRESHOLD = 1.15  # max/min over a row's values, both arms together
GATED_METRICS = ("enqueue_throughput", "bulk_enqueue")  # rows the gate may withhold
GATEABLE = (
    "enqueue_throughput",
    "enqueue_latency",
    "e2e",
    "e2e_diagnostic",
    "e2e_legacy_threads",
    "e2e_probe",
    "bulk_enqueue",
)
SMOKE = False


def apply_smoke_mode() -> None:
    """Tiny counts to validate the pipeline. Never for published numbers."""
    global ENQUEUE_COUNT, LATENCY_COUNT, BULK_COUNT, DEFAULT_DEPTHS, SMOKE
    ENQUEUE_COUNT = 30
    LATENCY_COUNT = 30
    BULK_COUNT = 300
    DEFAULT_DEPTHS = (20, 60)
    SMOKE = True


def e2e_timeout(depth: int) -> int:
    """
    Seconds before an end-to-end run is declared failed. Ten tasks per
    second is far under any rate observed, so the bound only catches a
    stall; a worker that exits is caught on the next poll.
    """
    return 120 if SMOKE else max(900, depth // 10)


def poll_interval(depth: int) -> float:
    """
    Observer poll interval for a drain of `depth` rows. The count query
    scans the task table, so a fixed 50 ms would put an observer load on
    the database that grows with depth; the interval grows instead (50 ms
    at 2,000, 100 ms at 20,000, 500 ms at 100,000) and is recorded on the
    entry. It bounds the resolution of the stop time and nothing else.
    """
    return max(POLL_INTERVAL, depth / 200_000)


BACKENDS = {
    "ox": {
        "settings": "benchsite.settings_ox",
        "database": "bench_ox",
        "table": "django_ox_oxtask",
        "tasks_module": "benchsite.tasks_ox",
        "app": "django_ox",
        "worker_command": "ox_worker",
    },
    "tasksdb": {
        "settings": "benchsite.settings_tasksdb",
        "database": "bench_tasksdb",
        "table": "django_tasks_database_dbtaskresult",
        "tasks_module": "benchsite.tasks_tasksdb",
        "app": "django_tasks_db",
        "worker_command": "db_worker",
    },
}


def utc_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="milliseconds")


def loadavg() -> list[float]:
    return [round(x, 2) for x in os.getloadavg()]


# --------------------------------------------------------------------------
# Roles: run inside a fresh subprocess, one measurement each.
# --------------------------------------------------------------------------


def role_setup(backend_name: str):
    """Boot Django on the arm's settings; return its workload module."""
    os.environ["DJANGO_SETTINGS_MODULE"] = BACKENDS[backend_name]["settings"]
    sys.path.insert(0, str(BENCH_DIR))
    import django

    django.setup()
    import importlib

    return importlib.import_module(BACKENDS[backend_name]["tasks_module"])


def role_delete_all(backend_name: str) -> None:
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute(f'DELETE FROM "{BACKENDS[backend_name]["table"]}"')


def role_analyze(backend_name: str) -> str:
    """
    ANALYZE the arm's task table on the role's own connection. Returns the
    wall time it finished, which the entry records.
    """
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute(f'ANALYZE "{BACKENDS[backend_name]["table"]}"')
    return utc_now()


def role_prepare_producer(backend_name: str, enqueue) -> str:
    """
    Untimed preparation shared by the producer cells: WARMUP_COUNT calls of
    the enqueue under test, delete those rows, ANALYZE the now empty table.
    Returns the ANALYZE timestamp.
    """
    for _ in range(WARMUP_COUNT):
        enqueue()
    role_delete_all(backend_name)
    return role_analyze(backend_name)


def role_enqueue_throughput(backend_name: str, count: int) -> None:
    """Time `count` sequential enqueues in autocommit mode."""
    noop = role_setup(backend_name).noop
    analyze_at = role_prepare_producer(backend_name, noop.enqueue)

    start = time.perf_counter()
    for _ in range(count):
        noop.enqueue()
    elapsed = time.perf_counter() - start
    print(
        json.dumps(
            {
                "seconds": elapsed,
                "tasks_per_sec": count / elapsed,
                "analyze_at": analyze_at,
            }
        )
    )


def role_enqueue_latency(backend_name: str, count: int) -> None:
    """
    Latency of a single enqueue() call made inside transaction.atomic().

    Each iteration opens its own atomic block; the timed window covers only
    the enqueue() call (the INSERT on the open transaction), not COMMIT.
    """
    noop = role_setup(backend_name).noop
    from django.db import transaction

    analyze_at = role_prepare_producer(backend_name, noop.enqueue)

    latencies_ms = []
    for _ in range(count):
        with transaction.atomic():
            start = time.perf_counter()
            noop.enqueue()
            latencies_ms.append((time.perf_counter() - start) * 1000)
    print(json.dumps({"latencies_ms": latencies_ms, "analyze_at": analyze_at}))


def role_enqueue_batch(backend_name: str, count: int) -> None:
    """
    Loop preload for the end-to-end cells (--preload loop): `count` calls
    of enqueue(), one autocommitted INSERT each. Outside the cell's clock;
    its own duration is reported for the record.
    """
    noop = role_setup(backend_name).noop
    start = time.perf_counter()
    for _ in range(count):
        noop.enqueue()
    print(
        json.dumps(
            {
                "preloaded": count,
                "seconds": time.perf_counter() - start,
                "method": "enqueue() loop, one autocommitted INSERT per row",
            }
        )
    )


def role_preload(backend_name: str, count: int) -> None:
    """
    Bulk preload for the end-to-end cells (the default): `count` READY rows
    written through each backend's own bulk path, one transaction, chunks
    of 1,000 rows per INSERT. Outside the cell's clock; its own duration is
    reported for the record.

    ox: django_ox.bulk.enqueue_many, which builds each row with the same
    code enqueue() uses. tasks-db has no bulk API, so the rows are built
    with the same columns its DatabaseBackend.enqueue() writes and inserted
    with the ORM's bulk_create. bulk_create sends no pre_save signal, so
    run_after is set to the value its receiver would have set (get_date_max
    stands for "no run_after"); enqueued_at comes from auto_now_add, as on
    enqueue().
    """
    noop = role_setup(backend_name).noop
    from django.db import transaction

    start = time.perf_counter()
    if backend_name == "ox":
        from django_ox.bulk import INSERT_CHUNK_SIZE, enqueue_many

        enqueue_many(noop, [((), {}) for _ in range(count)])
        method = (
            "django_ox.bulk.enqueue_many: bulk_create in chunks of "
            f"{INSERT_CHUNK_SIZE} rows, one transaction"
        )
    else:
        from django_tasks_db.compat import normalize_json
        from django_tasks_db.models import DBTaskResult, get_date_max

        backend = noop.get_backend()
        rows = [
            DBTaskResult(
                args_kwargs=normalize_json({"args": [], "kwargs": {}}),
                priority=noop.priority,
                task_path=noop.module_path,
                queue_name=noop.queue_name,
                run_after=get_date_max(),
                backend_name=backend.alias,
            )
            for _ in range(count)
        ]
        with transaction.atomic():
            DBTaskResult.objects.bulk_create(rows, batch_size=1000)
        method = (
            "DBTaskResult.objects.bulk_create in chunks of 1000 rows, one "
            "transaction, columns as DatabaseBackend.enqueue() writes them"
        )
    print(
        json.dumps(
            {
                "preloaded": count,
                "seconds": time.perf_counter() - start,
                "method": method,
            }
        )
    )


def role_enqueue_retry(backend_name: str, count: int) -> None:
    """Enqueue flaky(1..count), untimed: the workload of the retry row."""
    flaky = role_setup(backend_name).flaky
    for n in range(1, count + 1):
        flaky.enqueue(n)
    print(json.dumps({"enqueued": count}))


def role_bulk(backend_name: str, count: int, arm: str) -> None:
    """
    Bulk enqueue: `count` calls of item(n) written inside one outer
    transaction.atomic(). The payload list is built before the clock. The
    clock starts before the atomic block is entered and stops after it
    exits, so COMMIT is inside the window; the row count is read after it.

    arm "many" (ox only): django_ox.bulk.enqueue_many, one bulk_create in
      chunks of INSERT_CHUNK_SIZE rows. Inside the outer block its own
      atomic() is a savepoint, and it sends task_enqueued once per row
      after the insert, inside the window.
    arm "loop": item.enqueue(n) once per payload, either backend.
    """
    item = role_setup(backend_name).item
    from django.db import connection, transaction

    analyze_at = role_prepare_producer(backend_name, lambda: item.enqueue(0))
    payloads = list(range(count))
    if arm == "many":
        from django_ox.bulk import INSERT_CHUNK_SIZE, enqueue_many

        calls = [((n,), {}) for n in payloads]
        start = time.perf_counter()
        with transaction.atomic():
            enqueue_many(item, calls)
        elapsed = time.perf_counter() - start
        method = (
            "django_ox.bulk.enqueue_many inside one outer atomic(); "
            f"bulk_create in chunks of {INSERT_CHUNK_SIZE} rows"
        )
        chunk_size = INSERT_CHUNK_SIZE
    else:
        start = time.perf_counter()
        with transaction.atomic():
            for n in payloads:
                item.enqueue(n)
        elapsed = time.perf_counter() - start
        method = "item.enqueue(n) loop inside one outer atomic(), one INSERT per row"
        chunk_size = None
    with connection.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM "{BACKENDS[backend_name]["table"]}"')
        rows_after = cursor.fetchone()[0]
    print(
        json.dumps(
            {
                "seconds": elapsed,
                "tasks_per_sec": count / elapsed,
                "rows_after": rows_after,
                "validated": rows_after == count,
                "method": method,
                "chunk_size": chunk_size,
                "analyze_at": analyze_at,
            }
        )
    )


def role_probe(backend_name: str) -> None:
    """
    Report which Task class and backend class this arm resolves to, and
    the settings in force. Not a measurement: it records, from inside the
    arm's own settings, what the workload module and TASKS actually bound
    to, the backend OPTIONS, the worker command's argument defaults and,
    for ox, the timing constants the worker derives from them.
    """
    module = role_setup(backend_name)
    noop = module.noop
    backend = noop.get_backend()
    cfg = BACKENDS[backend_name]

    def qualname(obj) -> str:
        return f"{type(obj).__module__}.{type(obj).__qualname__}"

    from django.core.management import load_command_class

    command = load_command_class(cfg["app"], cfg["worker_command"])
    parser = command.create_parser("manage.py", cfg["worker_command"])
    # db_worker's --worker-id default is a fresh random string on every
    # parse, so it is a value of this probe, not a setting; left out.
    defaults = {
        action.dest: action.default
        for action in parser._actions
        if action.default is not argparse.SUPPRESS
        and action.dest not in ("help", "worker_id")
    }
    out = {
        "task_class": qualname(noop),
        "backend_class": qualname(backend),
        "backend_options": dict(backend.options),
        "worker_command_defaults": defaults,
    }
    if backend_name == "ox":
        from django_ox.worker import Worker

        try:
            worker = Worker()
            out["worker"] = {
                "max_attempts": backend.max_attempts,
                "lock_timeout": worker.lock_timeout,
                "reap_interval": worker.reap_interval,
                "renew_interval": worker.renew_interval,
                "backoff_initial": worker.backoff_initial,
                "backoff_max": worker.backoff_max,
                "poll_interval": worker.poll_interval,
            }
        except Exception as exc:  # recorded, not fatal: the probe is a report
            out["worker"] = {"error": repr(exc)}
    print(json.dumps(out, default=str))


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
    # CASCADE because oxscheduletick carries a nullable foreign key to
    # oxtask, and PostgreSQL refuses to truncate a table another one
    # references even when the referencing table is empty. Both benchmark
    # databases exist only for this harness, so cascading is a reset, not
    # a loss.
    cfg = BACKENDS[backend_name]
    with pg_connect(cfg["database"]) as conn:
        conn.execute(f'TRUNCATE "{cfg["table"]}" CASCADE')


def ensure_ledger(backend_name: str) -> None:
    """The retry row's side table, plain SQL in the arm's own database."""
    cfg = BACKENDS[backend_name]
    with pg_connect(cfg["database"]) as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} ("
            "id bigserial PRIMARY KEY, logical_id integer NOT NULL, "
            "pid integer NOT NULL, at timestamptz NOT NULL DEFAULT now())"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {LEDGER_TABLE}_logical_idx "
            f"ON {LEDGER_TABLE} (logical_id)"
        )


def truncate_ledger(backend_name: str) -> None:
    with pg_connect(BACKENDS[backend_name]["database"]) as conn:
        conn.execute(f"TRUNCATE {LEDGER_TABLE}")


def retry_rows(backend_name: str) -> list[dict]:
    """
    One record per logical task: its status, the backend's own attempt
    count (ox: the attempts column; tasks-db: the length of worker_ids,
    which grows by one per claim) and the ledger's execution count.
    """
    cfg = BACKENDS[backend_name]
    if backend_name == "ox":
        sql = (
            f'SELECT (args->>0)::int, status, attempts FROM "{cfg["table"]}" ORDER BY 1'
        )
    else:
        sql = (
            "SELECT (args_kwargs->'args'->>0)::int, status, "
            f'jsonb_array_length(worker_ids) FROM "{cfg["table"]}" ORDER BY 1'
        )
    with pg_connect(cfg["database"]) as conn:
        rows = conn.execute(sql).fetchall()
        ledger = dict(
            conn.execute(
                f"SELECT logical_id, count(*) FROM {LEDGER_TABLE} GROUP BY 1"
            ).fetchall()
        )
    return [
        {
            "logical_id": logical_id,
            "status": status,
            "attempts": attempts,
            "ledger_executions": ledger.get(logical_id, 0),
        }
        for logical_id, status, attempts in rows
    ]


def analyze(backend_name: str) -> str:
    """
    ANALYZE the arm's task table from the orchestrator, after a preload and
    before the timed spawn. Returns the wall time it finished.
    """
    cfg = BACKENDS[backend_name]
    with pg_connect(cfg["database"]) as conn:
        conn.execute(f'ANALYZE "{cfg["table"]}"')
    return utc_now()


def status_counts(backend_name: str) -> dict[str, int]:
    """Row count per status, one scan of the task table."""
    cfg = BACKENDS[backend_name]
    with pg_connect(cfg["database"]) as conn:
        rows = conn.execute(
            f'SELECT status, count(*) FROM "{cfg["table"]}" GROUP BY status'
        ).fetchall()
    return {status: count for status, count in rows}


def tail_lines(text: str, limit: int = 20, width: int = 400) -> list[str]:
    """
    The last `limit` lines of a process's output, each cut at `width`. A
    traceback names files by absolute path; the repository checkout, the
    virtualenv and the home directory are written as <repo>, <venv> and ~
    so a committed raw file carries no local path or account name.
    """
    replacements = (
        (str(BENCH_DIR.parent), "<repo>"),
        (sys.prefix, "<venv>"),
        (str(Path.home()), "~"),
    )
    lines = []
    for line in text.splitlines()[-limit:]:
        for old, new in replacements:
            line = line.replace(old, new)
        lines.append(line[:width])
    return lines


def shorten_command(command: str) -> str:
    """
    The interpreter (first token) reduced to its basename. ps reports the
    resolved framework binary rather than the venv symlink, so this is a
    basename cut, not a replacement of sys.executable.
    """
    interpreter, sep, rest = command.partition(" ")
    return Path(interpreter).name + sep + rest


def child_processes(pid: int) -> list[dict]:
    """
    The live children of `pid` (an ox supervisor): pid and command line.
    Read after the clock has stopped and before the stop signal, while the
    children are still up.
    """
    found = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True)
    pids = [int(x) for x in found.stdout.split()]
    if not pids:
        return []
    listing = subprocess.run(
        ["ps", "-o", "pid=,command=", "-p", ",".join(str(p) for p in pids)],
        capture_output=True,
        text=True,
    )
    children = []
    for line in listing.stdout.splitlines():
        child_pid, _, command = line.strip().partition(" ")
        if child_pid.isdigit():
            children.append(
                {"pid": int(child_pid), "command": shorten_command(command.strip())}
            )
    return children


def telemetry_start() -> dict:
    return {"started_at": utc_now(), "load_before": loadavg()}


def telemetry_end(entry: dict) -> dict:
    entry["finished_at"] = utc_now()
    entry["load_after"] = loadavg()
    return entry


def run_role(
    backend_name: str,
    role: str,
    count: int,
    log_name: str,
    extra_args: list[str] | None = None,
) -> dict:
    """
    Run a measurement role in a fresh subprocess; return its JSON result
    with a `process` block (pid, exit code, stderr tail) attached.
    """
    log_path = BENCH_DIR / "logs" / f"{log_name}.log"
    proc = subprocess.Popen(
        [
            sys.executable, str(BENCH_DIR / "bench.py"),
            "--role", role, "--backend", backend_name, "--count", str(count),
            *(extra_args or []),
        ],
        env=subprocess_env(backend_name),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, stderr = proc.communicate()
    log_path.write_text(
        f"# exit={proc.returncode}\n# stdout\n{stdout}\n# stderr\n{stderr}\n"
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"role {role} for {backend_name} failed (see {log_path}):\n{stderr[-2000:]}"
        )
    result = json.loads(stdout.strip().splitlines()[-1])
    result["process"] = {
        "pid": proc.pid,
        "exit_code": proc.returncode,
        "stderr_tail": tail_lines(stderr),
    }
    return result


def worker_commands(
    backend_name: str,
    processes: int,
    concurrency: int = 1,
    extra_args: list[str] | None = None,
) -> list[list[str]]:
    """
    Worker invocation per backend for `processes` workers.

    ox: one command line, `--processes P --concurrency C`. At P == 1 the
      command is the worker itself (ox_worker enters its supervisor only
      above 1), so `--processes 1 --concurrency 1` is the plain
      single-process worker spelled out. Above 1 the process started here
      is a supervisor that opens no database connection and starts P
      children, each `ox_worker --processes 1 --concurrency C`; one process
      is spawned by the harness, P workers execute tasks.
    tasksdb: db_worker has no concurrency option, so P worker processes.
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
                "--processes", str(processes),
                "--concurrency", str(concurrency), *extra,
            ]
        ]
    return [
        [sys.executable, "-m", "django", "db_worker", "--no-startup-delay", *extra]
        for _ in range(processes)
    ]


def stop_workers(procs: list[subprocess.Popen]) -> list[int]:
    """SIGTERM every live worker, wait, SIGKILL any that ignores it."""
    for proc in procs:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
    for proc in procs:
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    return [proc.returncode for proc in procs]


def run_e2e(
    backend_name: str,
    depth: int,
    processes: int,
    log_name: str,
    *,
    concurrency: int = 1,
    preload: str = "bulk",
    extra_args: list[str] | None = None,
) -> dict:
    """
    Pre-load `depth` tasks, ANALYZE the table, then measure from just before
    the worker process(es) are spawned until all rows are SUCCESSFUL.
    Includes worker process startup and Django init (identical procedure
    for both backends).
    """
    truncate(backend_name)
    pre = run_role(
        backend_name,
        "preload" if preload == "bulk" else "enqueue-batch",
        depth,
        f"{log_name}-preload",
    )
    ready = status_counts(backend_name).get("READY", 0)
    if ready != depth:
        raise RuntimeError(f"expected {depth} READY rows, found {ready}")
    analyze_at = analyze(backend_name)

    env = subprocess_env(backend_name)
    commands = worker_commands(backend_name, processes, concurrency, extra_args)
    interval = poll_interval(depth)
    timeout = e2e_timeout(depth)
    supervised = backend_name == "ox" and processes > 1
    outcome = {
        "backend": backend_name,
        "depth": depth,
        "processes": processes,
        "concurrency": concurrency,
        "topology": f"{processes}v{processes}",
        # The argv as run, interpreter shortened to its basename; tasksdb
        # runs `spawned_processes` identical copies of it.
        "worker_command": shlex.join([Path(commands[0][0]).name, *commands[0][1:]]),
        "spawned_processes": len(commands),
        "worker_processes": processes,
        "supervisor_process": supervised,
        "preload": pre,
        "analyze_at": analyze_at,
        "poll_interval": interval,
        "e2e_timeout": timeout,
        **telemetry_start(),
    }
    log_paths = [
        BENCH_DIR / "logs" / f"{log_name}-worker{i}.log" for i in range(len(commands))
    ]
    logs = []
    procs = []
    start = time.perf_counter()
    for cmd, log_path in zip(commands, log_paths, strict=True):
        log_file = open(log_path, "w")
        logs.append(log_file)
        procs.append(
            subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        )

    try:
        deadline = time.monotonic() + timeout
        while True:
            counts = status_counts(backend_name)
            successful = counts.get("SUCCESSFUL", 0)
            if successful >= depth:
                outcome["seconds"] = time.perf_counter() - start
                outcome["tasks_per_sec"] = depth / outcome["seconds"]
                break
            failed = counts.get("FAILED", 0)
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
                outcome["timeout_seconds"] = timeout
                outcome["error"] = (
                    f"timeout after {timeout}s with {successful} SUCCESSFUL"
                )
                break
            time.sleep(interval)
    finally:
        # Everything from here is after the clock stopped. The pids and
        # the supervisor's children are read while the processes are
        # still up; the exit codes after the stop signal. 0 is a clean
        # stop on SIGTERM; anything else means the worker died with a
        # traceback (see its log). Not part of any measurement.
        outcome["pids"] = [proc.pid for proc in procs]
        if supervised:
            outcome["child_processes"] = child_processes(procs[0].pid)
        outcome["worker_exit_codes"] = stop_workers(procs)
        for log_file in logs:
            log_file.close()
        # Worker stdout and stderr share one log file per process.
        outcome["stderr_tail"] = [
            tail_lines(path.read_text(errors="replace")) for path in log_paths
        ]
        telemetry_end(outcome)
    return outcome


def run_retry(backend_name: str, log_name: str) -> dict:
    """
    Behaviour row, not a timing. RETRY_COUNT flaky tasks, one worker at
    package defaults, observed until every task is terminal or
    RETRY_WINDOW seconds have passed. Records per task the terminal
    status, the backend's own attempt count and the ledger's execution
    count, plus a timeline of the status counts. No rate, no ranking.
    """
    truncate(backend_name)
    truncate_ledger(backend_name)
    producer = run_role(
        backend_name, "enqueue-retry", RETRY_COUNT, f"{log_name}-producer"
    )
    ready = status_counts(backend_name).get("READY", 0)
    if ready != RETRY_COUNT:
        raise RuntimeError(f"expected {RETRY_COUNT} READY rows, found {ready}")

    env = subprocess_env(backend_name)
    command = worker_commands(backend_name, 1)[0]
    outcome = {
        "backend": backend_name,
        "count": RETRY_COUNT,
        "window_seconds": RETRY_WINDOW,
        "worker_command": shlex.join([Path(command[0]).name, *command[1:]]),
        "producer": producer,
        "attempts_source": (
            "attempts column" if backend_name == "ox" else "len(worker_ids)"
        ),
        **telemetry_start(),
    }
    log_path = BENCH_DIR / "logs" / f"{log_name}-worker0.log"
    log_file = open(log_path, "w")
    proc = subprocess.Popen(command, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    start = time.perf_counter()
    deadline = time.monotonic() + RETRY_WINDOW
    timeline: list[dict] = []
    last: dict | None = None
    try:
        while True:
            counts = status_counts(backend_name)
            elapsed = time.perf_counter() - start
            if counts != last:
                timeline.append({"t": round(elapsed, 2), **counts})
                last = counts
            pending = counts.get("READY", 0) + counts.get("RUNNING", 0)
            if pending == 0:
                outcome["all_terminal"] = True
                outcome["observed_seconds"] = elapsed
                break
            if proc.poll() is not None:
                outcome["error"] = f"worker exited early (code {proc.returncode})"
                break
            if time.monotonic() > deadline:
                outcome["all_terminal"] = False
                outcome["observed_seconds"] = elapsed
                break
            time.sleep(0.5)
    finally:
        outcome["pids"] = [proc.pid]
        outcome["worker_exit_codes"] = stop_workers([proc])
        log_file.close()
        outcome["stderr_tail"] = [tail_lines(log_path.read_text(errors="replace"))]
        outcome["timeline"] = timeline
        outcome["status_counts"] = status_counts(backend_name)
        outcome["per_task"] = retry_rows(backend_name)
        summary: dict[str, int] = {}
        for row in outcome["per_task"]:
            key = (
                f"{row['status']} attempts={row['attempts']} "
                f"executions={row['ledger_executions']}"
            )
            summary[key] = summary.get(key, 0) + 1
        outcome["summary"] = summary
        telemetry_end(outcome)
    return outcome


def run_bulk(arm: str, run: int, position: int, order: list[str]) -> dict:
    """
    One bulk arm: truncate, run the role, then count the rows again from
    the orchestrator's own connection so the validation does not rest on
    the producer's word alone.
    """
    backend_name = "ox" if arm.startswith("ox") else "tasksdb"
    method = "many" if arm == "ox-many" else "loop"
    truncate(backend_name)
    telemetry = telemetry_start()
    r = run_role(
        backend_name, "bulk", BULK_COUNT, f"{arm}-bulk-run{run}",
        extra_args=["--arm", method],
    )
    counts = status_counts(backend_name)
    rows = sum(counts.values())
    r.update(
        arm=arm,
        backend=backend_name,
        run=run,
        count=BULK_COUNT,
        rows_after_orchestrator=rows,
        status_counts=counts,
        validated=bool(r.get("validated")) and rows == BULK_COUNT,
        position=position,
        order=list(order),
        **telemetry_end(telemetry),
    )
    return r


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile on the sorted sample."""
    ordered = sorted(values)
    rank = max(1, int(round(fraction * len(ordered) + 0.5)) - 1)
    return ordered[min(rank, len(ordered) - 1)]


def stability_row(
    values: list, *, expected_n: int, threshold: float, gated: bool
) -> dict:
    """
    One row of the stability summary: max/min over every value the row
    has, both arms together, against the threshold. A gated row whose
    ratio exceeds the threshold is marked withheld; its values stay.
    """
    clean = [float(v) for v in values if isinstance(v, (int, float)) and v > 0]
    row = {
        "values": clean,
        "n": len(clean),
        "expected_n": expected_n,
        "complete": len(clean) == expected_n,
        "max_over_min": None,
        "threshold": threshold,
        "gated": gated,
        "withheld_by_stability_gate": False,
        "reason": None,
    }
    if len(clean) >= 2:
        ratio = max(clean) / min(clean)
        row["max_over_min"] = round(ratio, 4)
        if gated and ratio > threshold:
            row["withheld_by_stability_gate"] = True
            row["reason"] = (
                f"max/min {ratio:.3f} over {len(clean)} values exceeds {threshold}"
            )
    return row


def stability_summary(
    results: dict, runs: int, threshold: float, gated: set[str]
) -> dict:
    """
    Recomputed at every checkpoint from the entries on file, so the raw
    file always carries the gate's verdict next to the values it judged.
    Only the metrics in `gated` can be withheld; every row carries the
    same fields so the page generator reads one shape.
    """

    def rates(entries: list[dict]) -> list:
        return [e.get("tasks_per_sec") for e in entries if "error" not in e]

    summary = {
        "enqueue_throughput": stability_row(
            rates(results.get("enqueue_throughput", [])),
            expected_n=2 * runs,
            threshold=threshold,
            gated="enqueue_throughput" in gated,
        )
    }
    for stat in ("p50_ms", "p95_ms"):
        summary[f"enqueue_latency_{stat[:3]}"] = stability_row(
            [e.get(stat) for e in results.get("enqueue_latency", [])],
            expected_n=2 * runs,
            threshold=threshold,
            gated="enqueue_latency" in gated,
        )
    groups: dict[tuple[int, int], list[dict]] = {}
    for e in results.get("e2e", []):
        groups.setdefault((e["depth"], e["processes"]), []).append(e)
    for (depth, processes), entries in sorted(groups.items()):
        summary[f"e2e_d{depth}_{processes}v{processes}"] = stability_row(
            rates(entries),
            expected_n=2 * runs,
            threshold=threshold,
            gated="e2e" in gated,
        )
    for section, expected in (
        ("e2e_diagnostic", runs),
        ("e2e_legacy_threads", 2 * runs),
        ("e2e_probe", 2),
    ):
        by_depth: dict[int, list[dict]] = {}
        for e in results.get(section, []):
            by_depth.setdefault(e["depth"], []).append(e)
        for depth, entries in sorted(by_depth.items()):
            summary[f"{section}_d{depth}"] = stability_row(
                rates(entries),
                expected_n=expected,
                threshold=threshold,
                gated=section in gated,
            )
    by_arm: dict[str, list[dict]] = {}
    for e in results.get("bulk_enqueue", []):
        by_arm.setdefault(e["arm"], []).append(e)
    for arm in BULK_ARMS:
        if arm in by_arm:
            # One row per arm: the three arms are three APIs, so a ratio
            # across them would measure the difference, not the noise.
            summary[f"bulk_enqueue_{arm}"] = stability_row(
                rates(by_arm[arm]),
                expected_n=runs,
                threshold=threshold,
                gated="bulk_enqueue" in gated,
            )
    return summary


def probe_summary(results: dict) -> dict:
    """
    The probe's publication rule, computed here rather than left to the
    page: per arm, the probe's drain rate against the arm's median 1v1
    rate at the deepest cell, and whether it holds within 20%. The quiet
    condition is read from the load averages on the probe entry itself,
    copied here; the verdict combines both.
    """
    probes = [e for e in results.get("e2e_probe", []) if "error" not in e]
    cells = [
        e
        for e in results.get("e2e", [])
        if "error" not in e and e["processes"] == 1
    ]
    if not probes or not cells:
        return {}
    deepest = max(e["depth"] for e in cells)
    out: dict = {
        "probe_depth": probes[0]["depth"],
        "deepest_cell_depth": deepest,
        "band": 0.20,
        "arms": {},
    }
    for e in probes:
        arm_rates = [
            c["tasks_per_sec"]
            for c in cells
            if c["backend"] == e["backend"] and c["depth"] == deepest
        ]
        if not arm_rates:
            continue
        median = statistics.median(arm_rates)
        ratio = e["tasks_per_sec"] / median
        out["arms"][e["backend"]] = {
            "probe_tasks_per_sec": e["tasks_per_sec"],
            "deepest_cell_median_tasks_per_sec": median,
            "deepest_cell_n": len(arm_rates),
            "ratio": round(ratio, 4),
            "holds_within_band": abs(ratio - 1) <= out["band"],
            "load_before": e["load_before"],
            "load_after": e["load_after"],
        }
    out["rate_holds_for_both_arms"] = bool(out["arms"]) and all(
        arm["holds_within_band"] for arm in out["arms"].values()
    )
    return out


def harness_revision() -> dict:
    def git(*args: str) -> str:
        out = subprocess.run(
            ["git", "-C", str(BENCH_DIR), *args], capture_output=True, text=True
        )
        return out.stdout.strip() if out.returncode == 0 else ""

    sha = git("rev-parse", "HEAD")
    return {
        "git_sha": sha or None,
        # True when anything under benchmarks/ differs from that commit.
        "git_dirty": bool(git("status", "--porcelain", "--", str(BENCH_DIR))),
    }


def collect_environment() -> dict:
    def sysctl(name: str) -> str:
        out = subprocess.run(
            ["/usr/sbin/sysctl", "-n", name], capture_output=True, text=True
        )
        return out.stdout.strip()

    def docker_value(*args: str) -> str | None:
        out = subprocess.run([*DOCKER, *args], capture_output=True, text=True)
        return out.stdout.strip() if out.returncode == 0 else None

    from importlib.metadata import PackageNotFoundError, version

    def installed(name: str) -> str | None:
        # None when the distribution is absent, so the block records what
        # the venv holds instead of raising. django-tasks-db 0.13.0 no
        # longer depends on the django-tasks backport, so that entry may be
        # null, or name a package neither arm imports; "resolved" below is
        # what each arm actually ran on.
        try:
            return version(name)
        except PackageNotFoundError:
            return None

    with pg_connect("postgres") as conn:
        pg_version = conn.execute("SHOW server_version").fetchone()[0]
        pg_settings = {
            name: conn.execute(f"SHOW {name}").fetchone()[0]
            for name in (
                "shared_buffers",
                "synchronous_commit",
                "fsync",
                "autovacuum",
                "max_connections",
            )
        }

    return {
        "cpu": sysctl("machdep.cpu.brand_string"),
        "memory_bytes": int(sysctl("hw.memsize") or 0),
        "logical_cpus": sysctl("hw.ncpu"),
        "macos": platform.mac_ver()[0],
        "python": platform.python_version(),
        "postgres_server": pg_version,
        "postgres_settings": pg_settings,
        "docker": {
            "image": IMAGE,
            "image_id": docker_value("inspect", "-f", "{{.Image}}", CONTAINER),
            "vm_cpus": docker_value("info", "--format", "{{.NCPU}}"),
            "vm_memory_bytes": docker_value("info", "--format", "{{.MemTotal}}"),
        },
        "harness": harness_revision(),
        "packages": {
            # django-ox is reported from the imported module, not from
            # importlib.metadata. Under an editable install the dist-info
            # version goes stale the moment __version__ changes, and a
            # results file that names the wrong version is worse than one
            # that names none: the number is right and the label lies.
            **{
                name: installed(name)
                for name in ("Django", "django-tasks-db", "django-tasks", "psycopg")
            },
            "django-ox": django_ox.__version__,
        },
        # Resolved inside each arm's own settings at run time (see role_probe).
        "resolved": {
            name: run_role(name, "probe", 0, f"{name}-probe") for name in BACKENDS
        },
    }


# --------------------------------------------------------------------------
# Schedule: which arm goes first, in which order the cells run.
# --------------------------------------------------------------------------


def arm_order(run: int, seed: int) -> list[str]:
    """
    AB, BA, AB, ... across runs: the arm that goes first alternates, and
    the seed picks which one opens run 1 (even: ox, odd: tasksdb).
    """
    first = ARMS[(run - 1 + seed) % 2]
    return [first, *(arm for arm in ARMS if arm != first)]


def bulk_order(run: int, seed: int) -> list[str]:
    """The three bulk arms rotate one place per run; the seed sets the start."""
    k = (run - 1 + seed) % len(BULK_ARMS)
    return [*BULK_ARMS[k:], *BULK_ARMS[:k]]


def build_plan(
    runs: int, seed: int, depths: list[int], cells: set[str], probe_depth: int | None
) -> list[dict]:
    """
    The whole session's order, computed before anything runs and recorded
    in the raw file as parameters.schedule. The orchestrator executes it
    step by step.
    """
    plan = []
    for run in range(1, runs + 1):
        order = arm_order(run, seed)
        if "throughput" in cells:
            plan.append({"run": run, "cell": "enqueue_throughput", "order": order})
        if "latency" in cells:
            plan.append({"run": run, "cell": "enqueue_latency", "order": order})
        if "e2e" in cells:
            for depth in depths:
                for processes in TOPOLOGIES:
                    plan.append(
                        {
                            "run": run,
                            "cell": "e2e",
                            "depth": depth,
                            "processes": processes,
                            "order": order,
                        }
                    )
        if "diagnostic" in cells:
            plan.append(
                {
                    "run": run,
                    "cell": "e2e_diagnostic",
                    "depth": depths[0],
                    "processes": 1,
                    "order": ["ox"],
                }
            )
        if "legacy" in cells:
            plan.append(
                {
                    "run": run,
                    "cell": "e2e_legacy_threads",
                    "depth": depths[0],
                    "order": order,
                }
            )
        if "bulk" in cells:
            plan.append(
                {"run": run, "cell": "bulk_enqueue", "order": bulk_order(run, seed)}
            )
        if "retry" in cells:
            plan.append({"run": run, "cell": "retry", "order": order})
    if "probe" in cells and probe_depth:
        plan.append(
            {
                "run": 1,
                "cell": "e2e_probe",
                "depth": probe_depth,
                "processes": 1,
                "order": arm_order(runs + 1, seed),
            }
        )
    return plan


def orchestrate(args: argparse.Namespace) -> None:
    smoke = args.smoke
    load = loadavg()
    quiet_gate = {
        "required": args.require_quiet,
        "threshold_one_minute_load": QUIET_LOAD,
        "load_at_start": load,
        "passed": None,
    }
    if args.require_quiet:
        if smoke:
            print(
                "smoke: --require-quiet ignored; one-minute load average is "
                f"{load[0]:.2f}"
            )
            quiet_gate["ignored_in_smoke"] = True
        elif load[0] >= QUIET_LOAD:
            sys.exit(
                f"Refusing to start a scored run: one-minute load average "
                f"{load[0]:.2f} is at or above {QUIET_LOAD:.0f} (--require-quiet). "
                f"Load averages now: {load[0]:.2f} {load[1]:.2f} {load[2]:.2f}. "
                "Stop what is running and start again."
            )
        else:
            quiet_gate["passed"] = True

    ensure_docker()
    ensure_container()
    ensure_databases()
    (BENCH_DIR / "logs").mkdir(exist_ok=True)
    for name in BACKENDS:
        print(f"Migrating {name}...")
        migrate(name)
        ensure_ledger(name)

    cells = set(args.cells)
    depths = list(args.depths)
    plan = build_plan(args.runs, args.seed, depths, cells, args.probe_depth)

    results = {
        "date": datetime.date.today().isoformat(),
        "environment": collect_environment(),
        "parameters": {
            "smoke": smoke,
            "enqueue_count": ENQUEUE_COUNT,
            "latency_count": LATENCY_COUNT,
            "warmup_count": WARMUP_COUNT,
            "bulk_count": BULK_COUNT,
            "bulk_arms": list(BULK_ARMS),
            "retry_count": RETRY_COUNT,
            "retry_window_seconds": RETRY_WINDOW,
            "depths": depths,
            "probe_depth": args.probe_depth,
            "topologies": [f"{p}v{p}" for p in TOPOLOGIES],
            "preload": args.preload,
            "poll_interval_rule": "max(0.05, depth / 200000) seconds",
            "runs": args.runs,
            "seed": args.seed,
            "cells": sorted(cells),
            "order": (
                "interleaved per run; the first arm alternates run by run "
                "(AB, BA, AB, ...), the seed picks the opener of run 1"
            ),
            "quiet_gate": quiet_gate,
            "stability_threshold": args.stability_threshold,
            "gated_metrics": sorted(args.gated),
            "schedule": plan,
        },
        "enqueue_throughput": [],
        "enqueue_latency": [],
        "e2e": [],
        "e2e_diagnostic": [],
        "e2e_legacy_threads": [],
        "e2e_probe": [],
        "bulk_enqueue": [],
        "retry": [],
        "stability": {},
        "probe": {},
    }
    sections = [
        "enqueue_throughput",
        "enqueue_latency",
        "e2e",
        "e2e_diagnostic",
        "e2e_legacy_threads",
        "e2e_probe",
        "bulk_enqueue",
        "retry",
    ]
    suffix = "-SMOKE" if smoke else ""
    out_path = BENCH_DIR / f"results-raw-{results['date']}{suffix}.json"
    if args.resume and out_path.exists():
        prior = json.loads(out_path.read_text())
        for key in sections:
            if key in prior:
                results[key] = prior[key]
        print(f"Resuming: loaded existing measurements from {out_path}")

    def have(section: str, **keys) -> bool:
        return any(
            all(entry.get(k) == v for k, v in keys.items())
            for entry in results.get(section, [])
        )

    def checkpoint():
        results["stability"] = stability_summary(
            results, args.runs, args.stability_threshold, set(args.gated)
        )
        results["probe"] = probe_summary(results)
        out_path.write_text(json.dumps(results, indent=2))

    def report_e2e(r: dict) -> None:
        if "error" in r:
            print(f"    ERROR: {r['error']}")
        else:
            print(f"    {r['seconds']:.1f}s ({r['tasks_per_sec']:.0f} tasks/sec)")

    checkpoint()
    for step in plan:
        cell, run, order = step["cell"], step["run"], step["order"]

        if cell == "enqueue_throughput":
            for position, name in enumerate(order, 1):
                if have(cell, backend=name, run=run):
                    continue
                print(f"[run {run}] {name}: enqueue throughput ({ENQUEUE_COUNT})...")
                truncate(name)
                telemetry = telemetry_start()
                r = run_role(
                    name, "enqueue-throughput", ENQUEUE_COUNT,
                    f"{name}-throughput-run{run}",
                )
                r.update(
                    backend=name, run=run, count=ENQUEUE_COUNT,
                    position=position, first_arm=order[0],
                    **telemetry_end(telemetry),
                )
                results[cell].append(r)
                print(f"    {r['tasks_per_sec']:.0f} tasks/sec ({r['seconds']:.2f}s)")
                checkpoint()

        elif cell == "enqueue_latency":
            for position, name in enumerate(order, 1):
                if have(cell, backend=name, run=run):
                    continue
                print(f"[run {run}] {name}: enqueue latency ({LATENCY_COUNT})...")
                truncate(name)
                telemetry = telemetry_start()
                r = run_role(
                    name, "enqueue-latency", LATENCY_COUNT, f"{name}-latency-run{run}"
                )
                lat = r.pop("latencies_ms")
                r.update(
                    backend=name,
                    run=run,
                    count=LATENCY_COUNT,
                    position=position,
                    first_arm=order[0],
                    p50_ms=percentile(lat, 0.50),
                    p95_ms=percentile(lat, 0.95),
                    mean_ms=statistics.fmean(lat),
                    min_ms=min(lat),
                    max_ms=max(lat),
                    latencies_ms=lat,
                    **telemetry_end(telemetry),
                )
                results[cell].append(r)
                print(f"    p50 {r['p50_ms']:.2f}ms  p95 {r['p95_ms']:.2f}ms")
                checkpoint()

        elif cell in ("e2e", "e2e_probe"):
            depth, processes = step["depth"], step["processes"]
            probe = cell == "e2e_probe"
            for position, name in enumerate(order, 1):
                if have(cell, backend=name, depth=depth, processes=processes, run=run):
                    continue
                label = "PROBE end-to-end" if probe else "end-to-end"
                print(
                    f"[run {run}] {name}: {label} {depth} tasks, "
                    f"{processes}v{processes} ({processes} worker process(es))..."
                )
                r = run_e2e(
                    name, depth, processes,
                    f"{name}-e2e-d{depth}-p{processes}-run{run}"
                    + ("-probe" if probe else ""),
                    preload=args.preload,
                )
                r.update(run=run, position=position, first_arm=order[0])
                if probe:
                    r["probe"] = True
                    r["label"] = (
                        f"probe: one 1v1 pair at depth {depth}, not a cell; "
                        "published only if its rate holds against the deepest cell"
                    )
                results[cell].append(r)
                report_e2e(r)
                checkpoint()

        elif cell == "e2e_diagnostic":
            depth = step["depth"]
            if have(cell, run=run, depth=depth):
                continue
            # Diagnostic row, NOT part of the defaults comparison. The
            # worker wakes as soon as an in-flight task settles, so the
            # poll interval governs only how often an idle worker looks
            # for new work. This run repeats 1v1 with --interval 0.1 to
            # check that property on every run: it should match the
            # default-interval cell.
            print(
                f"[run {run}] ox: DIAGNOSTIC end-to-end {depth} tasks, "
                "1v1, --interval 0.1 (non-default)..."
            )
            r = run_e2e(
                "ox", depth, 1, f"ox-e2e-d{depth}-p1-int01-run{run}",
                preload=args.preload, extra_args=["--interval", "0.1"],
            )
            r.update(run=run, position=1, first_arm="ox")
            r["label"] = "diagnostic: non-default --interval 0.1"
            results[cell].append(r)
            report_e2e(r)
            checkpoint()

        elif cell == "e2e_legacy_threads":
            depth = step["depth"]
            # The cell the matrix ranked before 2026-09-19: one ox process
            # with four threads against four tasks-db processes. Kept
            # runnable for the record (--legacy-threads); not a default cell.
            for position, name in enumerate(order, 1):
                if have(cell, backend=name, depth=depth, run=run):
                    continue
                print(
                    f"[run {run}] {name}: LEGACY end-to-end {depth} tasks, "
                    "ox 1 process x 4 threads vs tasksdb 4 processes..."
                )
                if name == "ox":
                    r = run_e2e(
                        name, depth, 1, f"{name}-e2e-d{depth}-legacy-run{run}",
                        concurrency=4, preload=args.preload,
                    )
                else:
                    r = run_e2e(
                        name, depth, 4, f"{name}-e2e-d{depth}-legacy-run{run}",
                        preload=args.preload,
                    )
                r.update(
                    run=run, position=position, first_arm=order[0],
                    topology="legacy: ox 1 process x 4 threads vs 4 tasksdb processes",
                    label="legacy cell, removed from the ranked matrix 2026-09-19",
                )
                results[cell].append(r)
                report_e2e(r)
                checkpoint()

        elif cell == "bulk_enqueue":
            for position, arm in enumerate(order, 1):
                if have(cell, arm=arm, run=run):
                    continue
                print(
                    f"[run {run}] {arm}: bulk enqueue {BULK_COUNT} "
                    "in one transaction..."
                )
                r = run_bulk(arm, run, position, order)
                results[cell].append(r)
                flag = "" if r["validated"] else "  NOT VALIDATED"
                print(
                    f"    {r['seconds']:.2f}s ({r['tasks_per_sec']:.0f} tasks/sec), "
                    f"{r['rows_after']} rows{flag}"
                )
                checkpoint()

        elif cell == "retry":
            for position, name in enumerate(order, 1):
                if have(cell, backend=name, run=run):
                    continue
                print(
                    f"[run {run}] {name}: retry behaviour, {RETRY_COUNT} tasks that "
                    f"raise once, one worker, observed up to {RETRY_WINDOW:.0f}s..."
                )
                r = run_retry(name, f"{name}-retry-run{run}")
                r.update(run=run, position=position, first_arm=order[0])
                results[cell].append(r)
                if "error" in r:
                    print(f"    ERROR: {r['error']}")
                else:
                    print(
                        f"    {r['status_counts']} after {r['observed_seconds']:.1f}s; "
                        f"{r['summary']}"
                    )
                checkpoint()

    checkpoint()
    print("\nStability gate (max/min over every value of the row, both arms):")
    for metric, row in results["stability"].items():
        if row["n"] < 2:
            continue
        verdict = "WITHHELD" if row["withheld_by_stability_gate"] else (
            "within threshold" if row["gated"] else "not gated"
        )
        print(
            f"  {metric}: {row['max_over_min']:.3f} over {row['n']} values, "
            f"{verdict}"
        )
    if results["probe"].get("arms"):
        probe = results["probe"]
        print(
            f"\nProbe at depth {probe['probe_depth']} against the "
            f"{probe['deepest_cell_depth']} 1v1 cell (band {probe['band']:.0%}):"
        )
        for arm, row in probe["arms"].items():
            print(
                f"  {arm}: {row['probe_tasks_per_sec']:.0f} vs median "
                f"{row['deepest_cell_median_tasks_per_sec']:.0f} tasks/sec, "
                f"ratio {row['ratio']:.3f}, "
                f"{'holds' if row['holds_within_band'] else 'does not hold'}"
            )
    print(f"\nRaw results written to {out_path}")
    print("Container left running; remove with:")
    print(f"  docker --context desktop-linux rm -f {CONTAINER}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--role",
        choices=[
            "enqueue-throughput",
            "enqueue-latency",
            "enqueue-batch",
            "preload",
            "bulk",
            "enqueue-retry",
            "probe",
        ],
    )
    parser.add_argument("--backend", choices=list(BACKENDS))
    parser.add_argument("--count", type=int, default=0)
    parser.add_argument(
        "--arm", choices=["many", "loop"], default="loop", help="bulk role only"
    )
    parser.add_argument("--runs", type=int, default=5)
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
    parser.add_argument(
        "--depth",
        default=None,
        help=(
            "Comma-separated READY row counts for the end-to-end cells "
            "(default 2000,20000; 20,60 under --smoke)."
        ),
    )
    parser.add_argument(
        "--probe-depth",
        type=int,
        default=None,
        help=(
            "Run one extra 1v1 pair at this depth after the matrix, recorded "
            "under e2e_probe as a probe, not a cell."
        ),
    )
    parser.add_argument(
        "--legacy-threads",
        action="store_true",
        help=(
            "Also run the pre-2026-09-19 cell (ox 1 process x 4 threads vs "
            "4 tasksdb processes), recorded under e2e_legacy_threads."
        ),
    )
    parser.add_argument(
        "--preload",
        choices=["bulk", "loop"],
        default="bulk",
        help="How the end-to-end rows are preloaded (outside the clock).",
    )
    parser.add_argument(
        "--require-quiet",
        action="store_true",
        help=(
            f"Refuse to start when the one-minute load average is >= {QUIET_LOAD:.0f}. "
            "Ignored under --smoke."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Picks which arm opens run 1 (even: ox, odd: tasksdb); recorded.",
    )
    parser.add_argument(
        "--stability-threshold",
        type=float,
        default=STABILITY_THRESHOLD,
        help="max/min over a row's values above which a gated row is withheld.",
    )
    parser.add_argument(
        "--gate",
        default=",".join(GATED_METRICS),
        help=(
            "Comma-separated metrics the stability gate may withhold "
            f"(default: {','.join(GATED_METRICS)}). Choose from: " + ", ".join(GATEABLE)
        ),
    )
    parser.add_argument(
        "--only",
        default=None,
        help=(
            "Comma-separated subset of cells to run: "
            + ", ".join(CELLS)
            + ". Default: every cell except legacy and probe, which their "
            "own flags enable."
        ),
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
            role_enqueue_batch(args.backend, args.count or DEFAULT_DEPTHS[0])
        elif args.role == "preload":
            role_preload(args.backend, args.count or DEFAULT_DEPTHS[0])
        elif args.role == "bulk":
            role_bulk(args.backend, args.count or BULK_COUNT, args.arm)
        elif args.role == "enqueue-retry":
            role_enqueue_retry(args.backend, args.count or RETRY_COUNT)
        elif args.role == "probe":
            role_probe(args.backend)
        return

    args.depths = (
        [int(x) for x in args.depth.split(",") if x.strip()]
        if args.depth
        else list(DEFAULT_DEPTHS)
    )
    cells = set(CELLS) - {"legacy", "probe"}
    if args.legacy_threads:
        cells.add("legacy")
    if args.probe_depth:
        cells.add("probe")
    if args.only:
        wanted = {x.strip() for x in args.only.split(",") if x.strip()}
        unknown = wanted - set(CELLS)
        if unknown:
            sys.exit(f"--only: unknown cell(s) {sorted(unknown)}; choose from {CELLS}")
        if "probe" in wanted and not args.probe_depth:
            sys.exit("--only probe needs --probe-depth")
        if "legacy" in wanted:
            cells.add("legacy")
        cells &= wanted
    args.cells = sorted(cells)
    args.gated = {x.strip() for x in args.gate.split(",") if x.strip()}
    unknown = args.gated - set(GATEABLE)
    if unknown:
        sys.exit(f"--gate: unknown metric(s) {sorted(unknown)}; choose from {GATEABLE}")

    orchestrate(args)


if __name__ == "__main__":
    main()
