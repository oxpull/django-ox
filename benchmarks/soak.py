#!/usr/bin/env python
"""
Soak and chaos harness: django-ox under sustained load and process kills.

Complements bench.py (throughput comparison) by testing the documented
reliability guarantees on PostgreSQL 16 over tens of minutes:

- **steady**: sustained mixed load from several producer processes against
  several worker processes, tracking enqueued vs completed counts, failure
  counts, queue depth over time, task latency percentiles, and worker RSS
  over time.
- **chaos**: the same load while worker processes are repeatedly SIGKILLed
  mid-task at random intervals and restarted, followed by ledger-based
  integrity assertions (every task terminal, reclaim within bound,
  attempts bounded, double executions measured and attributed).
- **crash-window**: a worker is killed while it provably holds claimed,
  unfinished tasks (forced via a task that sleeps), then restarted; the
  reclaim and re-execution are verified end to end.

Every task execution writes phase rows (started / completed / failed) with
a per-execution nonce to a side table (soak_ledger), so at-least-once vs
exactly-once behavior is measured from side effects, not assumed.

Run with a venv that has django-ox and psycopg installed, from this
directory, against the standing plow-pg container:

    docker run -d --name plow-pg -e POSTGRES_PASSWORD=plow -p 54329:5432 postgres:16
    python soak.py            # full run, roughly 25-30 minutes wall clock
    python soak.py --smoke    # pipeline validation with short durations

Raw results (exact invocation, per-scenario config, every sample, every
kill event, every assertion) checkpoint to soak-results-raw-<date>.json
after each scenario; per-process logs land in logs/. The published
SOAK-<date>.md is written by a person from the raw JSON.
"""

import argparse
import datetime
import json
import os
import platform
import random
import signal
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
LOG_DIR = BENCH_DIR / "logs"

PG = {
    "host": "127.0.0.1",
    "port": 54329,
    "user": "postgres",
    "password": "plow",
}
DATABASE = "soak_ox"
CONTAINER_RECIPE = (
    "docker run -d --name plow-pg -e POSTGRES_PASSWORD=plow "
    "-p 54329:5432 postgres:16"
)

TASK_TABLE = "django_ox_oxtask"
TICK_TABLE = "django_ox_oxscheduletick"
LEDGER_TABLE = "soak_ledger"

# Must match soaksite/settings.py OPTIONS. REAP_INTERVAL mirrors the
# worker's own derivation (min(30, max(LOCK_TIMEOUT / 2, 1))). The reclaim
# bound follows: a dead worker's lock expires LOCK_TIMEOUT after the
# claim, a surviving reaper notices within one reap interval, and the
# margin covers poll/claim/scheduling slack.
LOCK_TIMEOUT = 15.0
REAP_INTERVAL = min(30.0, max(LOCK_TIMEOUT / 2, 1.0))
RECLAIM_MARGIN = 15.0
RECLAIM_BOUND = LOCK_TIMEOUT + REAP_INTERVAL + RECLAIM_MARGIN
# A RUNNING lock older than this in a live sample means the reaper is not
# keeping up: LOCK_TIMEOUT plus one reap pass plus slack.
OVERDUE_AFTER = LOCK_TIMEOUT + REAP_INTERVAL + 5.0

WORKER_POLL_INTERVAL = 0.2
SAMPLE_INTERVAL = 5.0
TICK = 0.5
TERMINAL = ("SUCCESSFUL", "FAILED")

# Producer mix (name, weight). quick gets a jittered per-enqueue duration;
# the rest use their defaults from soaksite/tasks.py.
TASK_MIX = [
    ("quick", 70),
    ("medium", 15),
    ("slow", 5),
    ("flaky_once", 5),
    ("doomed", 5),
]


def scenario_configs(smoke: bool) -> list[dict]:
    if smoke:
        return [
            {
                "name": "steady", "kind": "load", "duration": 30.0,
                "producers": 2, "rate": 5.0, "workers": 2, "concurrency": 2,
                "chaos": None, "drain_timeout": 90.0,
            },
            {
                "name": "chaos", "kind": "load", "duration": 45.0,
                "producers": 2, "rate": 5.0, "workers": 2, "concurrency": 2,
                "chaos": {
                    "min_gap": 8.0, "max_gap": 14.0,
                    "restart_min": 1.0, "restart_max": 3.0,
                },
                "drain_timeout": 120.0,
            },
            {
                "name": "crash-window", "kind": "crash_window", "tasks": 4,
                "hold_seconds": 5.0, "concurrency": 2, "restart_delay": 3.0,
                "timeout": 120.0,
            },
        ]
    return [
        {
            "name": "steady", "kind": "load", "duration": 720.0,
            "producers": 3, "rate": 10.0, "workers": 3, "concurrency": 4,
            "chaos": None, "drain_timeout": 240.0,
        },
        {
            "name": "chaos", "kind": "load", "duration": 540.0,
            "producers": 3, "rate": 10.0, "workers": 3, "concurrency": 4,
            "chaos": {
                "min_gap": 20.0, "max_gap": 45.0,
                "restart_min": 2.0, "restart_max": 5.0,
            },
            "drain_timeout": 300.0,
        },
        {
            "name": "crash-window", "kind": "crash_window", "tasks": 4,
            "hold_seconds": 8.0, "concurrency": 2, "restart_delay": 3.0,
            "timeout": 150.0,
        },
    ]


# --------------------------------------------------------------------------
# Roles: run inside a fresh subprocess, spawned by the orchestrator.
# --------------------------------------------------------------------------


def role_setup():
    os.environ["DJANGO_SETTINGS_MODULE"] = "soaksite.settings"
    sys.path.insert(0, str(BENCH_DIR))
    import django

    django.setup()
    import importlib

    return importlib.import_module("soaksite.tasks")


def role_produce(rate: float, duration: float, seed: int) -> None:
    """Enqueue the weighted task mix at a steady rate until the deadline."""
    tasks = role_setup()
    rng = random.Random(seed)
    names = [name for name, _ in TASK_MIX]
    weights = [weight for _, weight in TASK_MIX]
    counts: dict[str, int] = defaultdict(int)
    interval = 1.0 / rate
    start = time.monotonic()
    deadline = start + duration
    next_at = start
    while time.monotonic() < deadline:
        name = rng.choices(names, weights=weights)[0]
        if name == "quick":
            tasks.quick.enqueue(seconds=round(rng.uniform(0.01, 0.05), 3))
        else:
            getattr(tasks, name).enqueue()
        counts[name] += 1
        next_at += interval
        delay = next_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)
    print(json.dumps({"enqueued": sum(counts.values()), "by_type": dict(counts)}))


def role_produce_batch(task_name: str, count: int, seconds: float) -> None:
    """Enqueue `count` tasks of one type, as fast as possible."""
    tasks = role_setup()
    task_fn = getattr(tasks, task_name)
    for _ in range(count):
        task_fn.enqueue(seconds=seconds)
    print(json.dumps({"enqueued": count, "by_type": {task_name: count}}))


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


def soak_env() -> dict:
    env = os.environ.copy()
    env["DJANGO_SETTINGS_MODULE"] = "soaksite.settings"
    env["PYTHONPATH"] = str(BENCH_DIR) + os.pathsep + env.get("PYTHONPATH", "")
    return env


def iso_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds")


def ensure_database() -> None:
    try:
        with pg_connect("postgres") as conn:
            exists = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE,)
            ).fetchone()
            if not exists:
                conn.execute(f'CREATE DATABASE "{DATABASE}"')
    except Exception as exc:
        sys.exit(
            f"PostgreSQL not reachable on {PG['host']}:{PG['port']} ({exc}).\n"
            f"Start the standing container first:\n  {CONTAINER_RECIPE}"
        )


def migrate() -> None:
    subprocess.run(
        [sys.executable, "-m", "django", "migrate", "-v", "0"],
        env=soak_env(),
        check=True,
        cwd=BENCH_DIR,
    )


def ensure_ledger() -> None:
    with pg_connect(DATABASE) as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
                id bigserial PRIMARY KEY,
                task_id uuid NOT NULL,
                attempt integer NOT NULL,
                nonce text NOT NULL,
                pid integer NOT NULL,
                phase text NOT NULL,
                at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS soak_ledger_task_idx "
            f"ON {LEDGER_TABLE} (task_id, at)"
        )


def reset_state() -> None:
    with pg_connect(DATABASE) as conn:
        conn.execute(f'TRUNCATE "{TICK_TABLE}", "{TASK_TABLE}", {LEDGER_TABLE}')


def db_counts() -> dict:
    with pg_connect(DATABASE) as conn:
        rows = conn.execute(
            f'SELECT "status", count(*) FROM "{TASK_TABLE}" GROUP BY "status"'
        ).fetchall()
        overdue = conn.execute(
            f'SELECT count(*) FROM "{TASK_TABLE}" '
            f"WHERE \"status\" = 'RUNNING' "
            f'AND "locked_at" < now() - make_interval(secs => %s)',
            (OVERDUE_AFTER,),
        ).fetchone()[0]
        ledger = conn.execute(f"SELECT count(*) FROM {LEDGER_TABLE}").fetchone()[0]
    counts = {status: n for status, n in rows}
    for status in ("READY", "RUNNING", "SUCCESSFUL", "FAILED"):
        counts.setdefault(status, 0)
    counts["overdue_running"] = overdue
    counts["ledger_rows"] = ledger
    return counts


def process_rss_kb(pids: list[int]) -> dict[int, int]:
    if not pids:
        return {}
    out = subprocess.run(
        ["ps", "-o", "pid=,rss=", "-p", ",".join(str(p) for p in pids)],
        capture_output=True,
        text=True,
    )
    rss = {}
    for line in out.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2:
            rss[int(parts[0])] = int(parts[1])
    return rss


class WorkerFleet:
    """Spawns, SIGKILLs, restarts, and drains ox_worker processes."""

    def __init__(self, prefix: str, count: int, concurrency: int, events: list):
        self.prefix = prefix
        self.count = count
        self.concurrency = concurrency
        self.events = events
        self.slots: list[dict | None] = [None] * count
        self.gens = [0] * count
        self.killed_pids: list[int] = []
        self.unexpected_exits: list[dict] = []

    def _event(self, event: str, slot: int, **extra) -> None:
        entry = self.slots[slot]
        self.events.append(
            {
                "at": iso_now(),
                "event": event,
                "slot": slot,
                "gen": self.gens[slot],
                "pid": entry["proc"].pid if entry else None,
                **extra,
            }
        )

    def start(self, slot: int) -> None:
        self.gens[slot] += 1
        log_path = LOG_DIR / f"{self.prefix}-worker{slot}-g{self.gens[slot]}.log"
        log = open(log_path, "w")
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "django", "ox_worker",
                "--concurrency", str(self.concurrency),
                "--interval", str(WORKER_POLL_INTERVAL),
            ],
            env=soak_env(),
            cwd=BENCH_DIR,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.slots[slot] = {"proc": proc, "log": log}
        self._event("worker_started", slot)

    def start_all(self) -> None:
        for slot in range(self.count):
            if self.slots[slot] is None:
                self.start(slot)

    def kill(self, slot: int) -> int:
        """SIGKILL the worker in `slot`; returns the killed pid."""
        entry = self.slots[slot]
        pid = entry["proc"].pid
        self._event("worker_killed", slot)
        entry["proc"].send_signal(signal.SIGKILL)
        entry["proc"].wait()
        entry["log"].close()
        self.slots[slot] = None
        self.killed_pids.append(pid)
        return pid

    def live_slots(self) -> list[int]:
        return [
            slot
            for slot, entry in enumerate(self.slots)
            if entry is not None and entry["proc"].poll() is None
        ]

    def check_unexpected_exits(self) -> None:
        """A dead process we did not kill is a finding, not chaos."""
        for slot, entry in enumerate(self.slots):
            if entry is not None and entry["proc"].poll() is not None:
                detail = {
                    "slot": slot,
                    "gen": self.gens[slot],
                    "pid": entry["proc"].pid,
                    "returncode": entry["proc"].returncode,
                }
                self.unexpected_exits.append(detail)
                self._event("worker_exited_unexpectedly", slot, **detail)
                entry["log"].close()
                self.slots[slot] = None
                self.start(slot)

    def rss_samples(self) -> list[dict]:
        pids = {
            entry["proc"].pid: slot
            for slot, entry in enumerate(self.slots)
            if entry is not None and entry["proc"].poll() is None
        }
        rss = process_rss_kb(list(pids))
        return [
            {"slot": pids[pid], "gen": self.gens[pids[pid]], "pid": pid, "rss_kb": kb}
            for pid, kb in rss.items()
        ]

    def stop_all(self) -> None:
        for slot in self.live_slots():
            self._event("worker_stopping", slot)
            self.slots[slot]["proc"].send_signal(signal.SIGTERM)
        for entry in self.slots:
            if entry is None:
                continue
            try:
                entry["proc"].wait(timeout=90)
            except subprocess.TimeoutExpired:
                entry["proc"].kill()
                entry["proc"].wait()
            entry["log"].close()
        self.slots = [None] * self.count


def take_sample(elapsed: float, fleet: WorkerFleet) -> dict:
    counts = db_counts()
    return {"t": iso_now(), "elapsed_s": round(elapsed, 1), **counts,
            "rss": fleet.rss_samples()}


def rss_summary(samples: list[dict]) -> list[dict]:
    """First/last/max RSS per (slot, generation), from the samples."""
    series: dict[tuple[int, int], list[tuple[float, int]]] = defaultdict(list)
    for sample in samples:
        for entry in sample.get("rss", []):
            series[(entry["slot"], entry["gen"])].append(
                (sample["elapsed_s"], entry["rss_kb"])
            )
    summary = []
    for (slot, gen), points in sorted(series.items()):
        kbs = [kb for _, kb in points]
        summary.append(
            {
                "slot": slot, "gen": gen, "samples": len(points),
                "first_kb": kbs[0], "last_kb": kbs[-1], "max_kb": max(kbs),
                "first_at_s": points[0][0], "last_at_s": points[-1][0],
            }
        )
    return summary


def parse_producer(entry: dict) -> dict:
    proc = entry["proc"]
    text = Path(entry["path"]).read_text()
    result = {"returncode": proc.returncode, "log": entry["path"].name}
    lines = [line for line in text.splitlines() if line.strip()]
    try:
        result.update(json.loads(lines[-1]))
    except (IndexError, json.JSONDecodeError):
        result["error"] = f"no JSON summary in {entry['path'].name}"
    return result


# --------------------------------------------------------------------------
# Analysis: everything asserted from the database and the ledger.
# --------------------------------------------------------------------------


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile on the sorted sample."""
    ordered = sorted(values)
    rank = max(1, int(round(fraction * len(ordered) + 0.5)) - 1)
    return ordered[min(rank, len(ordered) - 1)]


def analyze(
    killed_pids: list[int],
    producers: list[dict],
    samples: list[dict],
    drained: bool,
    unexpected_exits: list[dict],
    expected_interrupts: int | None = None,
) -> dict:
    with pg_connect(DATABASE) as conn:
        task_rows = conn.execute(
            f'SELECT "id"::text, "task_path", "status", "attempts", '
            f'"max_attempts", jsonb_array_length("worker_ids"), "enqueued_at", '
            f'"finished_at", "errors"::text FROM "{TASK_TABLE}"'
        ).fetchall()
        ledger_rows = conn.execute(
            f'SELECT "task_id"::text, "attempt", "nonce", "pid", "phase", "at" '
            f"FROM {LEDGER_TABLE} ORDER BY \"at\""
        ).fetchall()

    tasks = {}
    status_counts: dict[str, int] = defaultdict(int)
    for tid, path, status, attempts, max_attempts, wid_len, enq, fin, errors in (
        task_rows
    ):
        tasks[tid] = {
            "type": path.rsplit(".", 1)[-1],
            "status": status,
            "attempts": attempts,
            "max_attempts": max_attempts,
            "worker_ids_len": wid_len,
            "enqueued_at": enq,
            "finished_at": fin,
            "errors": errors,
        }
        status_counts[status] += 1

    executions: dict[tuple[str, str], dict] = {}
    started_by_task: dict[str, list] = defaultdict(list)
    completions_by_task: dict[str, list] = defaultdict(list)
    for tid, attempt, nonce, pid, phase, at in ledger_rows:
        key = (tid, nonce)
        if phase == "started":
            executions[key] = {"task_id": tid, "pid": pid, "started": at,
                               "attempt": attempt, "exit": None}
            started_by_task[tid].append(at)
        else:
            if key in executions:
                executions[key]["exit"] = phase
            if phase == "completed":
                completions_by_task[tid].append((at, pid))

    assertions: list[dict] = []

    def check(name: str, passed: bool, observed: str) -> None:
        assertions.append({"assertion": name, "passed": passed,
                           "observed": observed})

    # 1. Every enqueued row reached the database, one for one.
    enqueued = sum(p.get("enqueued", 0) for p in producers)
    check(
        "every enqueued task has a row",
        enqueued == len(tasks) and all(p.get("returncode") == 0 for p in producers),
        f"producers reported {enqueued}, table has {len(tasks)}",
    )

    # 2. Every task terminal after the drain.
    nonterminal = [t for t in tasks.values() if t["status"] not in TERMINAL]
    check(
        "all tasks terminal after drain",
        drained and not nonterminal,
        f"drained={drained}, non-terminal rows={len(nonterminal)}",
    )

    # 3. No RUNNING lock ever outlived LOCK_TIMEOUT plus a reap pass.
    max_overdue = max((s["overdue_running"] for s in samples), default=0)
    check(
        f"no lock older than {OVERDUE_AFTER:.1f}s in any live sample",
        max_overdue == 0,
        f"max overdue RUNNING rows across {len(samples)} samples: {max_overdue}",
    )

    # 4. Attempts bounded, and the attempts == len(worker_ids) invariant.
    over_attempts = [t for t in tasks.values() if t["attempts"] > t["max_attempts"]]
    check(
        "attempts never exceed max_attempts",
        not over_attempts,
        f"violations: {len(over_attempts)}",
    )
    mismatched = [t for t in tasks.values() if t["attempts"] != t["worker_ids_len"]]
    check(
        "attempts == len(worker_ids) on every row",
        not mismatched,
        f"violations: {len(mismatched)}",
    )

    # 5. Ledger vs outcome: every SUCCESSFUL task really ran, and double
    # executions are measured and attributed.
    phantom = [
        tid
        for tid, t in tasks.items()
        if t["status"] == "SUCCESSFUL" and not completions_by_task.get(tid)
    ]
    check(
        "every SUCCESSFUL task has >= 1 ledger completion",
        not phantom,
        f"phantom successes: {len(phantom)}",
    )
    histogram: dict[int, int] = defaultdict(int)
    for tid in completions_by_task:
        histogram[len(completions_by_task[tid])] += 1
    doubles = [
        {
            "task_id": tid,
            "type": tasks[tid]["type"],
            "completions": [
                {"at": at.isoformat(), "pid": pid}
                for at, pid in completions_by_task[tid]
            ],
        }
        for tid, completions in completions_by_task.items()
        if len(completions) > 1
    ]
    if killed_pids:
        # Only a kill between the ledger "completed" INSERT and the status
        # UPDATE can produce a duplicate completion; the earlier completion
        # must therefore come from a killed pid.
        unattributed = [
            d
            for d in doubles
            if not any(c["pid"] in killed_pids for c in d["completions"][:-1])
        ]
        check(
            "double completions only from killed workers",
            not unattributed,
            f"double-completed tasks: {len(doubles)}, "
            f"unattributed to a kill: {len(unattributed)}",
        )
    else:
        check(
            "exactly-once completions without chaos",
            not doubles,
            f"double-completed tasks: {len(doubles)}",
        )

    # 6. Interrupted executions (started, no exit row) were reclaimed and
    # re-executed, or correctly abandoned when out of attempts. Delays are
    # measured DB-clock to DB-clock (ledger timestamps only), so container
    # vs host clock drift cannot pollute them.
    interrupted = [e for e in executions.values() if e["exit"] is None]
    reclaim_delays: list[float] = []
    unresolved = []
    for execution in interrupted:
        later = [
            at
            for at in started_by_task[execution["task_id"]]
            if at > execution["started"]
        ]
        if later:
            reclaim_delays.append(
                (min(later) - execution["started"]).total_seconds()
            )
        else:
            row = tasks[execution["task_id"]]
            abandoned = (
                row["status"] == "FAILED" and "TaskAbandoned" in row["errors"]
            )
            if not abandoned:
                unresolved.append(execution["task_id"])
    check(
        "interrupted executions re-executed or abandoned",
        not unresolved,
        f"interrupted: {len(interrupted)}, re-executed: {len(reclaim_delays)}, "
        f"abandoned: {len(interrupted) - len(reclaim_delays) - len(unresolved)}, "
        f"unresolved: {len(unresolved)}",
    )
    max_delay = max(reclaim_delays, default=None)
    check(
        f"re-execution within {RECLAIM_BOUND:.1f}s of interrupted start",
        max_delay is None or max_delay <= RECLAIM_BOUND,
        "no interrupted executions were re-executed"
        if max_delay is None
        else f"max delay {max_delay:.1f}s over {len(reclaim_delays)} reclaims",
    )
    if expected_interrupts is not None:
        check(
            "kill interrupted the expected number of in-flight tasks",
            len(interrupted) == expected_interrupts,
            f"expected {expected_interrupts}, observed {len(interrupted)}",
        )

    # 7. Kills that landed between the claim UPDATE and body entry show up
    # as claims with no ledger "started" row. Reported, not asserted: they
    # are legitimate at-least-once behavior and terminality is covered
    # above; zero is the common case because the window is microseconds.
    missing_starts = sum(
        max(0, t["attempts"] - len(started_by_task.get(tid, [])))
        for tid, t in tasks.items()
    )

    # 8. Failure accounting per type: doomed must fail; anything else that
    # failed must be chaos-attributable (TaskAbandoned after kills consumed
    # its attempts), never an organic error.
    per_type: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for t in tasks.values():
        per_type[t["type"]][t["status"]] += 1
    doomed_rows = [t for t in tasks.values() if t["type"] == "doomed"]
    check(
        "all doomed tasks FAILED with attempts == max_attempts",
        all(
            t["status"] == "FAILED" and t["attempts"] == t["max_attempts"]
            for t in doomed_rows
        ),
        f"doomed: {len(doomed_rows)}, "
        f"failed: {sum(t['status'] == 'FAILED' for t in doomed_rows)}",
    )
    organic_failures = [
        tid
        for tid, t in tasks.items()
        if t["type"] != "doomed"
        and t["status"] == "FAILED"
        and "TaskAbandoned" not in t["errors"]
        and t["type"] != "flaky_once"
    ]
    flaky_failed = [
        tid
        for tid, t in tasks.items()
        if t["type"] == "flaky_once" and t["status"] == "FAILED"
    ]
    check(
        "no organic failures outside doomed",
        not organic_failures,
        f"unexplained FAILED rows: {len(organic_failures)}, "
        f"flaky_once FAILED (chaos-consumed attempts): {len(flaky_failed)}",
    )

    # 9. Workers only ever died when we killed them.
    check(
        "no unexpected worker exits",
        not unexpected_exits,
        f"unexpected exits: {len(unexpected_exits)}",
    )

    # Latency percentiles: enqueue to finish, first-attempt successes only
    # (retries include deliberate backoff and would measure the backoff
    # schedule, not the queue).
    latency: dict[str, dict] = {}
    by_type_samples: dict[str, list[float]] = defaultdict(list)
    for t in tasks.values():
        if t["status"] == "SUCCESSFUL" and t["attempts"] == 1 and t["finished_at"]:
            seconds = (t["finished_at"] - t["enqueued_at"]).total_seconds()
            by_type_samples[t["type"]].append(seconds)
            by_type_samples["all"].append(seconds)
    for name, values in sorted(by_type_samples.items()):
        latency[name] = {
            "count": len(values),
            "p50_s": round(percentile(values, 0.50), 3),
            "p95_s": round(percentile(values, 0.95), 3),
            "p99_s": round(percentile(values, 0.99), 3),
            "max_s": round(max(values), 3),
        }

    return {
        "status_counts": dict(status_counts),
        "per_type": {k: dict(v) for k, v in sorted(per_type.items())},
        "enqueued_by_producers": enqueued,
        "task_rows": len(tasks),
        "executions": len(executions),
        "ledger_rows": len(ledger_rows),
        "completions_histogram": dict(sorted(histogram.items())),
        "double_completed": doubles,
        "interrupted_executions": len(interrupted),
        "reclaim_delays_s": [round(d, 2) for d in sorted(reclaim_delays)],
        "reclaim_bound_s": RECLAIM_BOUND,
        "claim_window_missing_starts": missing_starts,
        "max_overdue_running": max_overdue,
        "max_ready_backlog": max((s["READY"] for s in samples), default=0),
        "latency_first_attempt": latency,
        "assertions": assertions,
    }


# --------------------------------------------------------------------------
# Scenario runners.
# --------------------------------------------------------------------------


def run_load_scenario(cfg: dict, seed: int) -> dict:
    rng = random.Random(seed)
    reset_state()
    events: list[dict] = []
    samples: list[dict] = []
    prefix = f"soak-{cfg['name']}"
    fleet = WorkerFleet(prefix, cfg["workers"], cfg["concurrency"], events)
    producers = []
    chaos = cfg["chaos"]
    kills = 0
    scenario = {"config": cfg, "seed": seed, "started_at": iso_now()}
    wall_start = time.monotonic()
    drained = False
    try:
        fleet.start_all()
        start = time.monotonic()
        for i in range(cfg["producers"]):
            log_path = LOG_DIR / f"{prefix}-producer{i}.log"
            log = open(log_path, "w")
            proc = subprocess.Popen(
                [
                    sys.executable, str(BENCH_DIR / "soak.py"),
                    "--role", "produce",
                    "--rate", str(cfg["rate"]),
                    "--duration", str(cfg["duration"]),
                    "--seed", str(seed + 100 + i),
                ],
                env=soak_env(),
                cwd=BENCH_DIR,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            producers.append({"proc": proc, "log": log, "path": log_path})

        load_deadline = start + cfg["duration"]
        next_kill = (
            start + rng.uniform(chaos["min_gap"], chaos["max_gap"]) if chaos else None
        )
        pending_restarts: list[tuple[float, int]] = []
        last_sample = 0.0
        while True:
            now = time.monotonic()
            producers_done = all(p["proc"].poll() is not None for p in producers)
            if (now >= load_deadline and producers_done) or (
                now >= load_deadline + 90
            ):
                break
            if chaos and now < load_deadline and now >= next_kill:
                live = fleet.live_slots()
                if live:
                    slot = rng.choice(live)
                    fleet.kill(slot)
                    kills += 1
                    pending_restarts.append(
                        (
                            now + rng.uniform(
                                chaos["restart_min"], chaos["restart_max"]
                            ),
                            slot,
                        )
                    )
                next_kill = now + rng.uniform(chaos["min_gap"], chaos["max_gap"])
            for due, slot in list(pending_restarts):
                if now >= due:
                    fleet.start(slot)
                    pending_restarts.remove((due, slot))
            fleet.check_unexpected_exits()
            if now - last_sample >= SAMPLE_INTERVAL:
                samples.append(take_sample(now - start, fleet))
                last_sample = now
            time.sleep(TICK)

        load_seconds = time.monotonic() - start
        # Drain: full fleet, no more kills, until the queue is empty.
        for _, slot in pending_restarts:
            fleet.start(slot)
        pending_restarts.clear()
        fleet.start_all()
        drain_deadline = time.monotonic() + cfg["drain_timeout"]
        while time.monotonic() < drain_deadline:
            now = time.monotonic()
            fleet.check_unexpected_exits()
            if now - last_sample >= SAMPLE_INTERVAL:
                samples.append(take_sample(now - start, fleet))
                last_sample = now
            counts = db_counts()
            if counts["READY"] + counts["RUNNING"] == 0:
                drained = True
                break
            time.sleep(TICK)
        samples.append(take_sample(time.monotonic() - start, fleet))
    finally:
        fleet.stop_all()
        for p in producers:
            if p["proc"].poll() is None:
                p["proc"].kill()
                p["proc"].wait()
            p["log"].close()

    producer_results = [parse_producer(p) for p in producers]
    scenario.update(
        load_seconds=round(load_seconds, 1),
        wall_seconds=round(time.monotonic() - wall_start, 1),
        kills=kills,
        drained=drained,
        producers=producer_results,
        events=events,
        rss=rss_summary(samples),
        samples=samples,
        analysis=analyze(
            fleet.killed_pids,
            producer_results,
            samples,
            drained,
            fleet.unexpected_exits,
        ),
    )
    return scenario


def run_crash_window_scenario(cfg: dict, seed: int) -> dict:
    """
    Kill a worker while it provably holds claimed, unfinished tasks.

    One worker with two slots, four slow_hold tasks: the worker claims two
    and sleeps inside them, the orchestrator kills it mid-sleep, restarts
    it, and the restarted worker must finish the two READY leftovers at
    once and reclaim the two stale RUNNING rows after LOCK_TIMEOUT.
    """
    reset_state()
    events: list[dict] = []
    samples: list[dict] = []
    fleet = WorkerFleet("soak-crash", 1, cfg["concurrency"], events)
    scenario = {"config": cfg, "seed": seed, "started_at": iso_now()}
    wall_start = time.monotonic()
    drained = False
    running_at_kill = 0
    try:
        fleet.start_all()
        producer_log = LOG_DIR / "soak-crash-producer.log"
        result = subprocess.run(
            [
                sys.executable, str(BENCH_DIR / "soak.py"),
                "--role", "produce-batch",
                "--task", "slow_hold",
                "--count", str(cfg["tasks"]),
                "--seconds", str(cfg["hold_seconds"]),
            ],
            env=soak_env(),
            cwd=BENCH_DIR,
            capture_output=True,
            text=True,
        )
        producer_log.write_text(
            f"# exit={result.returncode}\n{result.stdout}\n{result.stderr}\n"
        )
        if result.returncode != 0:
            raise RuntimeError(f"batch producer failed, see {producer_log}")
        producer_results = [json.loads(result.stdout.strip().splitlines()[-1])]
        producer_results[0]["returncode"] = 0

        start = time.monotonic()
        claim_deadline = start + 30
        while time.monotonic() < claim_deadline:
            running_at_kill = db_counts()["RUNNING"]
            if running_at_kill >= min(cfg["concurrency"], cfg["tasks"]):
                break
            time.sleep(0.2)
        time.sleep(1.0)  # well inside the hold sleep, past body entry
        running_at_kill = db_counts()["RUNNING"]
        samples.append(take_sample(time.monotonic() - start, fleet))
        fleet.kill(0)
        time.sleep(cfg["restart_delay"])
        fleet.start(0)

        deadline = start + cfg["timeout"]
        last_sample = 0.0
        while time.monotonic() < deadline:
            now = time.monotonic()
            fleet.check_unexpected_exits()
            if now - last_sample >= 2.0:
                samples.append(take_sample(now - start, fleet))
                last_sample = now
            counts = db_counts()
            if counts["READY"] + counts["RUNNING"] == 0:
                drained = True
                break
            time.sleep(TICK)
        samples.append(take_sample(time.monotonic() - start, fleet))
    finally:
        fleet.stop_all()

    scenario.update(
        load_seconds=round(time.monotonic() - wall_start, 1),
        wall_seconds=round(time.monotonic() - wall_start, 1),
        kills=1,
        drained=drained,
        running_at_kill=running_at_kill,
        producers=producer_results,
        events=events,
        rss=rss_summary(samples),
        samples=samples,
        analysis=analyze(
            fleet.killed_pids,
            producer_results,
            samples,
            drained,
            fleet.unexpected_exits,
            expected_interrupts=running_at_kill,
        ),
    )
    return scenario


# --------------------------------------------------------------------------
# Orchestration.
# --------------------------------------------------------------------------


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
            name: version(name) for name in ("Django", "django-ox", "psycopg")
        },
    }


def orchestrate(smoke: bool, only: list[str] | None, seed: int) -> None:
    ensure_database()
    LOG_DIR.mkdir(exist_ok=True)
    migrate()
    ensure_ledger()

    results = {
        "date": datetime.date.today().isoformat(),
        "started_at": iso_now(),
        "invocation": " ".join(sys.argv),
        "seed": seed,
        "parameters": {
            "lock_timeout_s": LOCK_TIMEOUT,
            "reap_interval_s": REAP_INTERVAL,
            "reclaim_bound_s": RECLAIM_BOUND,
            "overdue_after_s": OVERDUE_AFTER,
            "worker_poll_interval_s": WORKER_POLL_INTERVAL,
            "sample_interval_s": SAMPLE_INTERVAL,
            "task_mix": TASK_MIX,
        },
        "environment": collect_environment(),
        "scenarios": [],
    }
    suffix = "-SMOKE" if smoke else ""
    out_path = BENCH_DIR / f"soak-results-raw-{results['date']}{suffix}.json"

    failed_assertions = 0
    for cfg in scenario_configs(smoke):
        if only and cfg["name"] not in only:
            continue
        print(f"[{iso_now()}] scenario {cfg['name']}: starting...")
        if cfg["kind"] == "load":
            scenario = run_load_scenario(cfg, seed)
        else:
            scenario = run_crash_window_scenario(cfg, seed)
        results["scenarios"].append(scenario)
        out_path.write_text(json.dumps(results, indent=2))
        analysis = scenario["analysis"]
        print(
            f"[{iso_now()}] scenario {cfg['name']}: "
            f"load {scenario['load_seconds']}s, wall {scenario['wall_seconds']}s, "
            f"kills {scenario['kills']}, "
            f"tasks {analysis['task_rows']}, statuses {analysis['status_counts']}"
        )
        for assertion in analysis["assertions"]:
            mark = "PASS" if assertion["passed"] else "FAIL"
            if not assertion["passed"]:
                failed_assertions += 1
            print(f"    {mark}  {assertion['assertion']}: {assertion['observed']}")

    total_load = sum(s["load_seconds"] for s in results["scenarios"])
    total_wall = sum(s["wall_seconds"] for s in results["scenarios"])
    results["total_load_seconds"] = round(total_load, 1)
    results["total_wall_seconds"] = round(total_wall, 1)
    results["finished_at"] = iso_now()
    out_path.write_text(json.dumps(results, indent=2))
    print(
        f"\nTotal: {total_load / 60:.1f} min under load, "
        f"{total_wall / 60:.1f} min wall. Raw results: {out_path}"
    )
    if failed_assertions:
        sys.exit(f"{failed_assertions} assertion(s) FAILED; see {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", choices=["produce", "produce-batch"])
    parser.add_argument("--rate", type=float, default=10.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--task", default="slow_hold")
    parser.add_argument("--count", type=int, default=4)
    parser.add_argument("--seconds", type=float, default=8.0)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Pipeline validation with short durations; never for published numbers.",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=["steady", "chaos", "crash-window"],
        help="Run only the named scenario(s); default is all three.",
    )
    args = parser.parse_args()
    seed = args.seed if args.seed is not None else random.randrange(1_000_000)

    if args.role == "produce":
        role_produce(args.rate, args.duration, seed)
        return
    if args.role == "produce-batch":
        role_produce_batch(args.task, args.count, args.seconds)
        return

    orchestrate(args.smoke, args.scenario, seed)


if __name__ == "__main__":
    main()
