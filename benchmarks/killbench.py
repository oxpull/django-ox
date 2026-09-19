#!/usr/bin/env python
"""
Worker death harness: django-ox vs django-tasks-db under SIGKILL, as counts.

Two arms, one worker process each: `ox_worker --concurrency 1` and
`db_worker --no-startup-delay`. Neither arm runs under a supervisor; the
harness restarts the worker itself, identically in both arms. Per trial:

1. TRUNCATE the task table (CASCADE) and the kill_ledger side table.
2. Preload N due tasks whose body sleeps 100 ms and writes a "started" and
   an "effect" ledger row (benchsite/tasks_kill.py). ANALYZE the task table.
3. Spawn the worker. Once the first task is SUCCESSFUL, SIGKILL the worker
   K times at gaps drawn from a seeded RNG, only while READY rows remain,
   starting a replacement immediately after each kill. Right after each
   kill, read the RUNNING rows: with one dead worker and no live one, those
   rows are stranded, and each is attributed to the killed process from the
   task table itself (ox: locked_by carries the pid; tasks-db: the last
   worker_ids entry is the --worker-id the harness passed).
4. Drain until idle (no status change for 30 s and READY == 0), then observe
   120 s more with the worker alive, then SIGTERM it.
5. Read the task table and the ledger and count. Nothing is read from
   worker stdout.

Results go to results-kill-<date>.json (or -SMOKE.json under --smoke): every
trial, facts first, with medians derived afterwards by summarize() so the
per-trial records stay the source of truth. Self-checks fail the run when
the ledger and the task table disagree in a way that means the harness is
wrong, as opposed to the backend.

Run from this directory with the project venv:

    ../../.venv/bin/python killbench.py --smoke     # N=100, K=2, 1 trial
    ../../.venv/bin/python killbench.py             # N=2000, K=20, 5 trials
"""

import argparse
import datetime
import json
import os
import random
import re
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import bench

BENCH_DIR = Path(__file__).resolve().parent
LOG_DIR = BENCH_DIR / "logs"
LEDGER_TABLE = "kill_ledger"

# Must match benchsite/settings_kill_ox.py. The reap interval mirrors the
# worker's derivation; the reaper runs on the poll loop, so the reclaim of a
# stranded row is bounded by lease expiry plus one reap interval plus one
# poll interval of slack, on any live worker.
LOCK_TIMEOUT = 15.0
REAP_INTERVAL = min(30.0, max(LOCK_TIMEOUT / 2, 1.0))
OX_POLL_INTERVAL = 1.0
RECLAIM_BOUND = LOCK_TIMEOUT + REAP_INTERVAL + OX_POLL_INTERVAL

TASK_SLEEP = 0.1
# The observer's wait between reads is drawn from this range, never a fixed
# value and never the body's 100 ms: two processes sleeping the same period
# on one host get their wakeups coalesced, and a kill fired on such a tick
# lands at the same point of the task cycle every time. A kill that falls
# due inside a wait cuts the wait short, so it lands at the instant the RNG
# chose rather than on a tick boundary.
TICK_RANGE_S = (0.04, 0.12)
SAMPLE_INTERVAL = 5.0
IDLE_SECONDS = 30.0
OBSERVE_SECONDS = 120.0
STOP_TIMEOUT = 30.0
# Statuses after which nothing in either package runs the task again. LOST
# and DISCARDED exist only in django-ox.
TERMINAL = ("SUCCESSFUL", "FAILED", "LOST", "DISCARDED")
STATUS_KEYS = ("READY", "RUNNING", "SUCCESSFUL", "FAILED", "LOST")

FULL = {"n_tasks": 2000, "kills": 20, "trials": 5, "kill_gap_s": (3.0, 10.0)}
SMOKE = {"n_tasks": 100, "kills": 2, "trials": 1, "kill_gap_s": (2.5, 4.0)}

# bench.py's helpers (ensure_databases, migrate, truncate) read this table.
# Pointing it at the kill databases makes them create, migrate and truncate
# ours, and leaves bench.py's own databases alone. The two harnesses share
# the container, and a bench.py cell once truncated bench_tasksdb under a
# kill trial in progress; separate databases are the fix.
bench.BACKENDS["ox"].update(database="kill_ox", settings="benchsite.settings_kill_ox")
bench.BACKENDS["tasksdb"].update(
    database="kill_tasksdb", settings="benchsite.settings_kill_tasksdb"
)
HARNESS_TASK_PATH = "benchsite.tasks_kill.sleep_and_record"
ARMS = {
    "ox": {
        **bench.BACKENDS["ox"],
        "label": "django-ox",
        "tasks_module": "benchsite.tasks_kill",
    },
    "tasksdb": {
        **bench.BACKENDS["tasksdb"],
        "label": "django-tasks-db",
        "tasks_module": "benchsite.tasks_kill",
    },
}
OX_LOCKED_BY_PID = re.compile(r"-(\d+)-[A-Za-z0-9]{8}$")


def worker_command(arm: str, worker_name: str) -> list[str]:
    if arm == "ox":
        return [sys.executable, "-m", "django", "ox_worker", "--concurrency", "1"]
    return [
        sys.executable,
        "-m",
        "django",
        "db_worker",
        "--no-startup-delay",
        "--worker-id",
        worker_name,
    ]


def arm_env(arm: str, worker_name: str | None = None) -> dict:
    env = os.environ.copy()
    env["DJANGO_SETTINGS_MODULE"] = ARMS[arm]["settings"]
    env["PYTHONPATH"] = str(BENCH_DIR) + os.pathsep + env.get("PYTHONPATH", "")
    if worker_name is not None:
        env["KILLBENCH_WORKER"] = worker_name
    return env


def iso_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="milliseconds")


def iso(dt) -> str | None:
    return None if dt is None else dt.isoformat(timespec="milliseconds")


# --------------------------------------------------------------------------
# Role: preload, in a fresh subprocess with the arm's settings.
# --------------------------------------------------------------------------


def role_preload(arm: str, count: int) -> None:
    os.environ["DJANGO_SETTINGS_MODULE"] = ARMS[arm]["settings"]
    sys.path.insert(0, str(BENCH_DIR))
    import django

    django.setup()
    import importlib

    module = importlib.import_module(ARMS[arm]["tasks_module"])
    for _ in range(count):
        module.sleep_and_record.enqueue(seconds=TASK_SLEEP)
    print(json.dumps({"enqueued": count}))


def run_preload(arm: str, count: int, log_name: str) -> dict:
    log_path = LOG_DIR / f"{log_name}.log"
    result = subprocess.run(
        [
            sys.executable,
            str(BENCH_DIR / "killbench.py"),
            "--role",
            "preload",
            "--arm",
            arm,
            "--count",
            str(count),
        ],
        env=arm_env(arm),
        cwd=BENCH_DIR,
        capture_output=True,
        text=True,
    )
    log_path.write_text(
        f"# exit={result.returncode}\n# stdout\n{result.stdout}\n# stderr\n{result.stderr}\n"
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"preload for {arm} failed (see {log_path}):\n{result.stderr[-2000:]}"
        )
    summary = json.loads(result.stdout.strip().splitlines()[-1])
    summary["returncode"] = result.returncode
    return summary


# --------------------------------------------------------------------------
# Database side: ledger, observer.
# --------------------------------------------------------------------------


def recreate_ledger(arm: str) -> None:
    with bench.pg_connect(ARMS[arm]["database"]) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {LEDGER_TABLE}")
        conn.execute(
            f"""
            CREATE TABLE {LEDGER_TABLE} (
                id bigserial PRIMARY KEY,
                task_id uuid NOT NULL,
                nonce text NOT NULL,
                pid integer NOT NULL,
                worker_name text NOT NULL,
                attempt integer,
                phase text NOT NULL,
                at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """
        )
        conn.execute(
            f"CREATE INDEX {LEDGER_TABLE}_task_idx ON {LEDGER_TABLE} (task_id, at)"
        )
        conn.execute(f"CREATE INDEX {LEDGER_TABLE}_nonce_idx ON {LEDGER_TABLE} (nonce)")


def reset_ledger(arm: str) -> None:
    with bench.pg_connect(ARMS[arm]["database"]) as conn:
        conn.execute(f"TRUNCATE {LEDGER_TABLE}")


class Observer:
    """One autocommit connection per trial; every fact comes through it."""

    def __init__(self, arm: str):
        self.arm = arm
        self.table = ARMS[arm]["table"]
        self.conn = bench.pg_connect(ARMS[arm]["database"])

    def close(self) -> None:
        self.conn.close()

    def status_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            f'SELECT status, count(*) FROM "{self.table}" GROUP BY status'
        ).fetchall()
        counts = {status: n for status, n in rows}
        for key in STATUS_KEYS:
            counts.setdefault(key, 0)
        return counts

    def db_now(self):
        return self.conn.execute("SELECT clock_timestamp()").fetchone()[0]

    def analyze(self) -> None:
        self.conn.execute(f'ANALYZE "{self.table}"')

    def running_rows(self) -> list[dict]:
        if self.arm == "ox":
            rows = self.conn.execute(
                f"SELECT id::text, locked_by, attempts, lease_expires_at, "
                f'clock_timestamp() FROM "{self.table}" WHERE status = %s',
                ("RUNNING",),
            ).fetchall()
            return [
                {
                    "id": tid,
                    "claimant": locked_by,
                    "attempts": attempts,
                    "lease_expires_at": iso(expires),
                    "lease_remaining_s": (
                        None
                        if expires is None
                        else round((expires - now).total_seconds(), 3)
                    ),
                }
                for tid, locked_by, attempts, expires, now in rows
            ]
        rows = self.conn.execute(
            f'SELECT id::text, worker_ids FROM "{self.table}" WHERE status = %s',
            ("RUNNING",),
        ).fetchall()
        return [
            {
                "id": tid,
                "claimant": worker_ids[-1] if worker_ids else None,
                "attempts": len(worker_ids),
            }
            for tid, worker_ids in rows
        ]

    def task_rows(self) -> dict[str, dict]:
        if self.arm == "ox":
            rows = self.conn.execute(
                f"SELECT id::text, status, attempts, worker_ids, locked_by, "
                f'started_at, finished_at, errors, task_path FROM "{self.table}"'
            ).fetchall()
            return {
                tid: {
                    "status": status,
                    "attempts_column": attempts,
                    "worker_ids": worker_ids,
                    "claimant": locked_by,
                    "started_at": started,
                    "finished_at": finished,
                    "error": errors or None,
                    "task_path": path,
                }
                for (
                    tid,
                    status,
                    attempts,
                    worker_ids,
                    locked_by,
                    started,
                    finished,
                    errors,
                    path,
                ) in rows
            }
        rows = self.conn.execute(
            f"SELECT id::text, status, worker_ids, started_at, finished_at, "
            f'exception_class_path, task_path FROM "{self.table}"'
        ).fetchall()
        return {
            tid: {
                "status": status,
                "attempts_column": None,
                "worker_ids": worker_ids,
                "claimant": worker_ids[-1] if worker_ids else None,
                "started_at": started,
                "finished_at": finished,
                "error": exc or None,
                "task_path": path,
            }
            for tid, status, worker_ids, started, finished, exc, path in rows
        }

    def ledger_rows(self) -> list[tuple]:
        return self.conn.execute(
            f"SELECT task_id::text, nonce, pid, worker_name, attempt, phase, at "
            f"FROM {LEDGER_TABLE} ORDER BY at, id"
        ).fetchall()


# --------------------------------------------------------------------------
# Worker processes.
# --------------------------------------------------------------------------


def tail(path: Path, lines: int = 15) -> list[str]:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return []
    return text.splitlines()[-lines:]


def spawn_worker(arm: str, name: str, gen: int, log_prefix: str) -> dict:
    cmd = worker_command(arm, name)
    log_path = LOG_DIR / f"{log_prefix}-g{gen}.log"
    log = open(log_path, "w")
    proc = subprocess.Popen(
        cmd,
        env=arm_env(arm, name),
        cwd=BENCH_DIR,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    return {
        "gen": gen,
        "name": name,
        "pid": proc.pid,
        # Recorded with a plain "python": the results file ships, and
        # sys.executable is a local path.
        "command": ["python", *cmd[1:]],
        "spawned_at": iso_now(),
        "spawned_mono": time.monotonic(),
        "log": log_path.name,
        "exit_code": None,
        "ended_by": None,
        "ended_at": None,
        "stderr_tail": None,
        "proc": proc,
        "log_handle": log,
    }


def finish_worker(worker: dict, ended_by: str) -> None:
    proc = worker["proc"]
    worker["exit_code"] = proc.returncode
    worker["ended_by"] = ended_by
    worker["ended_at"] = iso_now()
    worker["log_handle"].close()
    worker["stderr_tail"] = tail(LOG_DIR / worker["log"])


def stop_worker(worker: dict) -> None:
    proc = worker["proc"]
    if proc.poll() is not None:
        finish_worker(worker, "exited before stop")
        return
    proc.terminate()
    try:
        proc.wait(timeout=STOP_TIMEOUT)
        finish_worker(worker, "SIGTERM")
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        finish_worker(worker, f"SIGKILL after {STOP_TIMEOUT:.0f}s SIGTERM timeout")


def claimant_matches(arm: str, claimant: str | None, worker: dict) -> bool:
    if claimant is None:
        return False
    if arm == "ox":
        match = OX_LOCKED_BY_PID.search(claimant)
        return bool(match) and int(match.group(1)) == worker["pid"]
    return claimant == worker["name"]


def kill_worker(
    arm: str, worker: dict, obs: Observer, k: int, gap: float, counts: dict
) -> dict:
    proc = worker["proc"]
    host_at = iso_now()
    proc.kill()
    proc.wait()
    db_at = obs.db_now()
    finish_worker(worker, "SIGKILL")
    running = obs.running_rows()
    stranded = [
        row for row in running if claimant_matches(arm, row["claimant"], worker)
    ]
    carried = [
        row for row in running if not claimant_matches(arm, row["claimant"], worker)
    ]
    return {
        "k": k,
        "planned_gap_s": gap,
        "gen": worker["gen"],
        "worker": worker["name"],
        "pid": worker["pid"],
        "at": host_at,
        "db_at": iso(db_at),
        "_db_at": db_at,
        "exit_code": proc.returncode,
        "counts_at_kill": counts,
        "stranded": stranded,
        "running_from_earlier_kills": carried,
    }


def public_worker(worker: dict) -> dict:
    return {
        k: v
        for k, v in worker.items()
        if k not in ("proc", "log_handle", "spawned_mono")
    }


# --------------------------------------------------------------------------
# One trial.
# --------------------------------------------------------------------------


def signature(counts: dict) -> tuple:
    return tuple(sorted(counts.items()))


def run_trial(
    arm: str, trial: int, seed: int, params: dict, windows: dict, smoke: bool
) -> dict:
    cfg = ARMS[arm]
    rng = random.Random(seed)
    gaps = [
        round(rng.uniform(*params["kill_gap_s"]), 3) for _ in range(params["kills"])
    ]
    prefix = f"kill{'-smoke' if smoke else ''}-{arm}-t{trial}"
    record = {
        "trial": trial,
        "arm": arm,
        "label": cfg["label"],
        "seed": seed,
        "planned_kill_gaps_s": gaps,
        "started_at": iso_now(),
        "load_before": os.getloadavg(),
    }
    n_tasks = params["n_tasks"]
    drain_timeout = max(600.0, n_tasks * TASK_SLEEP * 3 + params["kills"] * 30.0)

    bench.truncate(arm)
    reset_ledger(arm)
    record["preload"] = run_preload(arm, n_tasks, f"{prefix}-preload")
    obs = Observer(arm)
    ready = obs.status_counts()["READY"]
    if ready != n_tasks:
        raise RuntimeError(
            f"{arm}: expected {n_tasks} READY rows after preload, found {ready}"
        )
    record["preload"]["ready_before_spawn"] = ready
    obs.analyze()
    record["analyze_at"] = iso_now()

    workers: list[dict] = []
    kills: list[dict] = []
    samples: list[dict] = []
    unexpected_exits: list[dict] = []
    phases: dict = {}
    kills_not_delivered: list[dict] = []
    changes_during_observation: list[dict] = []

    def spawn() -> dict:
        gen = len(workers) + 1
        worker = spawn_worker(arm, f"{arm}-t{trial}-g{gen}", gen, prefix)
        workers.append(worker)
        return worker

    def replace_if_dead(current: dict, now_s: float) -> dict:
        if current["proc"].poll() is None:
            return current
        finish_worker(current, "unexpected exit")
        detail = {
            "gen": current["gen"],
            "pid": current["pid"],
            "exit_code": current["exit_code"],
            "at": current["ended_at"],
            "elapsed_s": round(now_s, 1),
        }
        unexpected_exits.append(detail)
        print(
            f"    unexpected exit: gen {detail['gen']} pid {detail['pid']} code {detail['exit_code']}"
        )
        return spawn()

    current = spawn()
    phases["first_spawn_at"] = current["spawned_at"]
    t0 = time.monotonic()
    first_success_mono = None
    next_kill_mono = None
    last_sig = None
    last_change_mono = t0
    last_sample = -SAMPLE_INTERVAL
    drained = False
    try:
        while True:
            now = time.monotonic()
            counts = obs.status_counts()
            sig = signature(counts)
            if sig != last_sig:
                last_sig = sig
                last_change_mono = now
            if first_success_mono is None and counts["SUCCESSFUL"] >= 1:
                first_success_mono = now
                phases["first_success_at"] = iso_now()
                if gaps:
                    next_kill_mono = now + gaps[0]
            current = replace_if_dead(current, now - t0)
            if (
                len(kills) < params["kills"]
                and first_success_mono is not None
                and now >= next_kill_mono
            ):
                if counts["READY"] > 0:
                    event = kill_worker(
                        arm, current, obs, len(kills) + 1, gaps[len(kills)], counts
                    )
                    current = spawn()
                    event["replacement"] = {
                        "gen": current["gen"],
                        "name": current["name"],
                        "pid": current["pid"],
                        "spawned_at": current["spawned_at"],
                    }
                    kills.append(event)
                    print(
                        f"    kill {event['k']}: pid {event['pid']} at +{now - t0:.1f}s, "
                        f"READY {counts['READY']}, stranded {len(event['stranded'])}, "
                        f"replacement pid {current['pid']}"
                    )
                    if len(kills) < params["kills"]:
                        next_kill_mono = time.monotonic() + gaps[len(kills)]
                else:
                    # Killing an idle worker strands nothing; the schedule is
                    # cut short and the shortfall recorded.
                    for k in range(len(kills) + 1, params["kills"] + 1):
                        kills_not_delivered.append(
                            {"k": k, "reason": "no READY rows left", "at": iso_now()}
                        )
                    next_kill_mono = float("inf")
            if now - last_sample >= SAMPLE_INTERVAL:
                samples.append({"t_s": round(now - t0, 1), **counts})
                last_sample = now
            if (
                counts["READY"] == 0
                and now - last_change_mono >= windows["idle_seconds"]
            ):
                drained = True
                break
            if now - t0 > drain_timeout:
                break
            wait = rng.uniform(*TICK_RANGE_S)
            if next_kill_mono is not None and len(kills) < params["kills"]:
                wait = min(wait, max(0.0, next_kill_mono - time.monotonic()))
            time.sleep(wait)

        phases["idle_confirmed_at" if drained else "drain_timeout_at"] = iso_now()
        phases["last_status_change_s"] = round(last_change_mono - t0, 1)
        phases["drain_elapsed_s"] = round(time.monotonic() - t0, 1)
        undelivered = {e["k"] for e in kills_not_delivered}
        for k in range(len(kills) + 1, params["kills"] + 1):
            if k not in undelivered:
                kills_not_delivered.append(
                    {
                        "k": k,
                        "reason": "drain ended before this kill was due",
                        "at": iso_now(),
                    }
                )
        samples.append({"t_s": round(time.monotonic() - t0, 1), **obs.status_counts()})

        observe_end = time.monotonic() + windows["observe_seconds"]
        while time.monotonic() < observe_end:
            now = time.monotonic()
            counts = obs.status_counts()
            sig = signature(counts)
            if sig != last_sig:
                last_sig = sig
                changes_during_observation.append({"t_s": round(now - t0, 1), **counts})
            current = replace_if_dead(current, now - t0)
            if now - last_sample >= SAMPLE_INTERVAL:
                samples.append({"t_s": round(now - t0, 1), **counts})
                last_sample = now
            time.sleep(0.5)
        phases["observation_end_at"] = iso_now()
    finally:
        stop_worker(current)
        record["load_after"] = os.getloadavg()

    tasks = obs.task_rows()
    ledger = obs.ledger_rows()
    samples.append({"t_s": round(time.monotonic() - t0, 1), **obs.status_counts()})
    obs.close()

    analysis = analyze(arm, tasks, ledger, workers, kills, n_tasks)
    for kill in kills:
        kill.pop("_db_at", None)
    record.update(
        drained=drained,
        drain_timeout_s=drain_timeout,
        phases=phases,
        kills_planned=params["kills"],
        kills_delivered=len(kills),
        kills_not_delivered=kills_not_delivered,
        kills=kills,
        workers=[public_worker(w) for w in workers],
        unexpected_exits=unexpected_exits,
        changes_during_observation=changes_during_observation,
        samples=samples,
        analysis=analysis,
        harness_ok=all(c["passed"] for c in analysis["self_checks"]),
        finished_at=iso_now(),
    )
    return record


# --------------------------------------------------------------------------
# Analysis: counts from the task table and the ledger.
# --------------------------------------------------------------------------


def analyze(
    arm: str, tasks: dict, ledger: list, workers: list, kills: list, n_tasks: int
) -> dict:
    status_counts: dict[str, int] = defaultdict(int)
    for row in tasks.values():
        status_counts[row["status"]] += 1
    for key in STATUS_KEYS:
        status_counts.setdefault(key, 0)

    executions: dict[str, dict] = {}
    inconsistent_nonces: list[str] = []
    for tid, nonce, pid, worker_name, attempt, phase, at in ledger:
        ex = executions.setdefault(
            nonce,
            {
                "nonce": nonce,
                "task_id": tid,
                "pid": pid,
                "worker": worker_name,
                "attempt": attempt,
                "started_at": None,
                "effect_at": None,
                "started_rows": 0,
                "effect_rows": 0,
                "other_rows": 0,
            },
        )
        if (ex["task_id"], ex["pid"], ex["worker"]) != (tid, pid, worker_name):
            inconsistent_nonces.append(nonce)
        if phase == "started":
            ex["started_rows"] += 1
            ex["started_at"] = ex["started_at"] or at
        elif phase == "effect":
            ex["effect_rows"] += 1
            ex["effect_at"] = ex["effect_at"] or at
        else:
            ex["other_rows"] += 1

    starts_by_task: dict[str, list[dict]] = defaultdict(list)
    effects_by_task: dict[str, list[dict]] = defaultdict(list)
    for ex in executions.values():
        if ex["started_at"] is not None:
            starts_by_task[ex["task_id"]].append(ex)
        if ex["effect_at"] is not None:
            effects_by_task[ex["task_id"]].append(ex)
    for lst in starts_by_task.values():
        lst.sort(key=lambda e: e["started_at"])
    for lst in effects_by_task.values():
        lst.sort(key=lambda e: e["effect_at"])

    killed_names = {k["worker"] for k in kills}
    spawned = {(w["pid"], w["name"]) for w in workers}
    kill_db_at = {k["worker"]: k["_db_at"] for k in kills}

    def describe(ex: dict) -> dict:
        return {
            "nonce": ex["nonce"],
            "task_id": ex["task_id"],
            "worker": ex["worker"],
            "pid": ex["pid"],
            "started_at": iso(ex["started_at"]),
            "effect_at": iso(ex["effect_at"]),
        }

    repeated_effects = [
        {
            "task_id": tid,
            "final_status": tasks[tid]["status"] if tid in tasks else None,
            "effects": [describe(e) for e in effects],
            "earlier_effects_all_from_killed_workers": all(
                e["worker"] in killed_names for e in effects[:-1]
            ),
        }
        for tid, effects in effects_by_task.items()
        if len(effects) > 1
    ]
    repeated_starts = [
        {
            "task_id": tid,
            "final_status": tasks[tid]["status"] if tid in tasks else None,
            "starts": [describe(e) for e in starts],
        }
        for tid, starts in starts_by_task.items()
        if len(starts) > 1
    ]
    partial_executions = [
        ex
        for ex in executions.values()
        if ex["started_at"] is not None and ex["effect_at"] is None
    ]
    lost_partials = [
        {
            "task_id": tid,
            "final_status": tasks[tid]["status"] if tid in tasks else None,
            "last_execution": describe(starts[-1]),
        }
        for tid, starts in starts_by_task.items()
        if starts[-1]["effect_at"] is None
    ]
    never_terminal = {
        tid: row["status"]
        for tid, row in tasks.items()
        if row["status"] not in TERMINAL
    }
    phantom_successes = [
        tid
        for tid, row in tasks.items()
        if row["status"] == "SUCCESSFUL" and not effects_by_task.get(tid)
    ]

    # Per kill: which window the kill landed in, and what happened to the
    # rows it stranded. Reclaim lags are DB clock to DB clock.
    kill_windows: dict[str, int] = defaultdict(int)
    reclaim_lags: list[float] = []
    stranded_never_restarted = 0
    stranded_total = 0
    stranded_by_task: dict[str, int] = {}
    mid_body_unstranded: list[int] = []
    for kill in kills:
        own = [ex for ex in executions.values() if ex["worker"] == kill["worker"]]
        last = (
            max(own, key=lambda e: e["started_at"] or e["effect_at"]) if own else None
        )
        stranded_ids = {row["id"] for row in kill["stranded"]}
        if last is None:
            window = (
                "before_first_start" if not stranded_ids else "after_claim_before_body"
            )
        elif last["effect_at"] is None:
            window = "mid_body"
        elif last["task_id"] in stranded_ids:
            window = "after_effect_before_outcome"
        elif stranded_ids:
            window = "after_claim_before_body"
        else:
            window = "between_tasks"
        kill["window"] = window
        kill["last_execution_of_killed_worker"] = describe(last) if last else None
        # Where in the task cycle the kill landed, DB clock to DB clock. Over
        # a campaign these should spread across the whole cycle; a pile-up
        # at one offset means the schedule is phase-locked to the worker.
        kill["ms_after_last_start"] = (
            round((kill["_db_at"] - last["started_at"]).total_seconds() * 1000, 1)
            if last is not None and last["started_at"] is not None
            else None
        )
        kill["ms_after_last_effect"] = (
            round((kill["_db_at"] - last["effect_at"]).total_seconds() * 1000, 1)
            if last is not None and last["effect_at"] is not None
            else None
        )
        kill_windows[window] += 1
        if window == "mid_body" and last["task_id"] not in stranded_ids:
            mid_body_unstranded.append(kill["k"])
        for row in kill["stranded"]:
            stranded_total += 1
            stranded_by_task[row["id"]] = kill["k"]
            later = [
                e["started_at"]
                for e in starts_by_task.get(row["id"], [])
                if e["started_at"] > kill["_db_at"]
            ]
            row["final_status"] = (
                tasks[row["id"]]["status"] if row["id"] in tasks else None
            )
            if later:
                lag = (min(later) - kill["_db_at"]).total_seconds()
                row["next_start_after_kill_s"] = round(lag, 3)
                reclaim_lags.append(lag)
            else:
                row["next_start_after_kill_s"] = None
                stranded_never_restarted += 1
        replacement = kill.get("replacement")
        if replacement:
            firsts = [
                ex["started_at"]
                for ex in executions.values()
                if ex["worker"] == replacement["name"] and ex["started_at"] is not None
            ]
            replacement["first_start_db_at"] = iso(min(firsts)) if firsts else None
            replacement["kill_to_first_start_s"] = (
                round((min(firsts) - kill["_db_at"]).total_seconds(), 3)
                if firsts
                else None
            )

    running_at_end = [
        {
            "task_id": tid,
            "stranded_by_kill": stranded_by_task.get(tid),
            "claimant": tasks[tid]["claimant"],
        }
        for tid, status in never_terminal.items()
        if status == "RUNNING"
    ]
    attempts_hist: dict[int, int] = defaultdict(int)
    for row in tasks.values():
        attempts_hist[len(row["worker_ids"] or [])] += 1
    failures = [
        {"task_id": tid, "status": row["status"], "error": row["error"]}
        for tid, row in tasks.items()
        if row["status"] in ("FAILED", "LOST")
    ]
    worker_first_starts = {}
    for worker in workers:
        firsts = [
            ex["started_at"]
            for ex in executions.values()
            if ex["worker"] == worker["name"] and ex["started_at"] is not None
        ]
        worker_first_starts[worker["name"]] = iso(min(firsts)) if firsts else None

    # Cycle shape, as context for the kill windows: how long the body took
    # between its two ledger rows, and how long a worker spent between one
    # task's effect row and the next task's started row (outcome write,
    # claim, hand-off to the body).
    body_ms = [
        (ex["effect_at"] - ex["started_at"]).total_seconds() * 1000
        for ex in executions.values()
        if ex["started_at"] is not None and ex["effect_at"] is not None
    ]
    by_worker: dict[str, list[dict]] = defaultdict(list)
    for ex in executions.values():
        if ex["started_at"] is not None:
            by_worker[ex["worker"]].append(ex)
    gap_ms = []
    for exs in by_worker.values():
        exs.sort(key=lambda e: e["started_at"])
        for prev, nxt in zip(exs, exs[1:], strict=False):
            if prev["effect_at"] is not None:
                gap_ms.append(
                    (nxt["started_at"] - prev["effect_at"]).total_seconds() * 1000
                )

    # Self-checks: harness correctness, not backend behaviour.
    checks: list[dict] = []

    def check(name: str, passed: bool, observed: str) -> None:
        checks.append({"check": name, "passed": bool(passed), "observed": observed})

    check(
        "task table holds exactly the preloaded rows",
        len(tasks) == n_tasks,
        f"preloaded {n_tasks}, rows {len(tasks)}",
    )
    foreign = [
        tid for tid, row in tasks.items() if row["task_path"] != HARNESS_TASK_PATH
    ]
    check(
        "every task row is a harness task (nothing else wrote to the table)",
        not foreign,
        f"rows with another task_path: {len(foreign)}",
    )
    check(
        "status counts add up to the row count",
        sum(status_counts.values()) == len(tasks),
        f"sum {sum(status_counts.values())}, rows {len(tasks)}",
    )
    unknown_workers = {
        (ex["pid"], ex["worker"])
        for ex in executions.values()
        if (ex["pid"], ex["worker"]) not in spawned
    }
    check(
        "every ledger row comes from a (pid, worker) the harness spawned",
        not unknown_workers,
        f"unknown: {sorted(unknown_workers)[:5]}",
    )
    unknown_tasks = {
        ex["task_id"] for ex in executions.values() if ex["task_id"] not in tasks
    }
    check(
        "every ledger task id exists in the task table",
        not unknown_tasks,
        f"unknown task ids: {len(unknown_tasks)}",
    )
    bad_nonces = [
        n
        for n, ex in executions.items()
        if ex["started_rows"] != 1
        or ex["effect_rows"] > 1
        or ex["other_rows"]
        or (ex["effect_at"] is not None and ex["effect_at"] < ex["started_at"])
    ]
    check(
        "each nonce has one started row and at most one effect row, effect after started",
        not bad_nonces and not inconsistent_nonces,
        f"malformed nonces: {len(bad_nonces)}, inconsistent: {len(inconsistent_nonces)}",
    )
    partial_by_worker: dict[str, int] = defaultdict(int)
    for ex in partial_executions:
        partial_by_worker[ex["worker"]] += 1
    over = {w: n for w, n in partial_by_worker.items() if n > 1}
    check(
        "no worker left more than one partial execution (one slot per worker)",
        not over,
        f"violations: {over}",
    )
    live_partials = {w for w in partial_by_worker if w not in killed_names}
    check(
        "every partial execution belongs to a killed worker",
        not live_partials,
        f"partials from unkilled workers: {sorted(live_partials)}",
    )
    check(
        "every SUCCESSFUL task has at least one effect row",
        not phantom_successes,
        f"SUCCESSFUL without effect: {len(phantom_successes)}",
    )
    late_rows = [
        (ex["worker"], ex["nonce"])
        for ex in executions.values()
        if ex["worker"] in kill_db_at
        and max(filter(None, (ex["started_at"], ex["effect_at"])))
        > kill_db_at[ex["worker"]]
    ]
    check(
        "no ledger row from a killed worker is later than its kill",
        not late_rows,
        f"late rows: {len(late_rows)}",
    )
    check(
        "every kill event has exit code -9 and at most one stranded row",
        all(k["exit_code"] == -9 and len(k["stranded"]) <= 1 for k in kills),
        f"kills: {len(kills)}, exit codes {[k['exit_code'] for k in kills]}, "
        f"max stranded {max((len(k['stranded']) for k in kills), default=0)}",
    )
    check(
        "a mid-body kill always shows its task RUNNING right after the kill",
        not mid_body_unstranded,
        f"mid-body kills without a matching stranded row: {mid_body_unstranded}",
    )

    return {
        "status_counts": dict(sorted(status_counts.items())),
        "task_rows": len(tasks),
        "never_terminal": len(never_terminal),
        "never_terminal_by_status": dict(
            sorted(Counter(never_terminal.values()).items())
        ),
        "running_at_end": running_at_end,
        "ledger_rows": len(ledger),
        "distinct_nonces": len(executions),
        "executions_per_task_histogram": dict(
            sorted(Counter(len(s) for s in starts_by_task.values()).items())
        ),
        "attempts_histogram": dict(sorted(attempts_hist.items())),
        "repeated_effects": repeated_effects,
        "repeated_effects_count": len(repeated_effects),
        "repeated_starts": repeated_starts,
        "repeated_starts_count": len(repeated_starts),
        "partial_executions": len(partial_executions),
        "lost_partials": lost_partials,
        "lost_partials_count": len(lost_partials),
        "failures": failures,
        "kill_windows": dict(sorted(kill_windows.items())),
        "kill_ms_after_last_start": [k["ms_after_last_start"] for k in kills],
        "body_ms_median": round(statistics.median(body_ms), 1) if body_ms else None,
        "inter_task_gap_ms_median": round(statistics.median(gap_ms), 1)
        if gap_ms
        else None,
        "stranded_rows": stranded_total,
        "stranded_never_restarted": stranded_never_restarted,
        "reclaim_lags_s": [round(x, 3) for x in sorted(reclaim_lags)],
        "reclaim_lag_median_s": round(statistics.median(reclaim_lags), 3)
        if reclaim_lags
        else None,
        "reclaim_lag_max_s": round(max(reclaim_lags), 3) if reclaim_lags else None,
        "reclaim_bound_s": RECLAIM_BOUND if arm == "ox" else None,
        "worker_first_start_db_at": worker_first_starts,
        "self_checks": checks,
    }


# --------------------------------------------------------------------------
# Derived numbers. The per-trial records are the source of truth.
# --------------------------------------------------------------------------


def summarize(trials: list[dict]) -> dict:
    by_arm: dict[str, list[dict]] = defaultdict(list)
    for t in trials:
        by_arm[t["arm"]].append(t)

    def series(ts: list[dict], pick) -> dict:
        values = [pick(t) for t in ts]
        present = [v for v in values if v is not None]
        return {
            "values": values,
            "median": statistics.median(present) if present else None,
        }

    def a(t: dict, key: str):
        return t["analysis"][key]

    summary = {}
    for arm, ts in by_arm.items():
        summary[arm] = {
            "trials": len(ts),
            "harness_ok_all": all(t["harness_ok"] for t in ts),
            "kills_delivered": series(ts, lambda t: t["kills_delivered"]),
            **{
                f"kills_{w}": series(ts, lambda t, w=w: a(t, "kill_windows").get(w, 0))
                for w in (
                    "mid_body",
                    "after_effect_before_outcome",
                    "after_claim_before_body",
                    "between_tasks",
                    "before_first_start",
                )
            },
            **{
                f"status_{s}": series(
                    ts, lambda t, s=s: a(t, "status_counts").get(s, 0)
                )
                for s in STATUS_KEYS
            },
            "never_terminal": series(ts, lambda t: a(t, "never_terminal")),
            "distinct_nonces": series(ts, lambda t: a(t, "distinct_nonces")),
            "repeated_effects": series(ts, lambda t: a(t, "repeated_effects_count")),
            "repeated_starts": series(ts, lambda t: a(t, "repeated_starts_count")),
            "lost_partials": series(ts, lambda t: a(t, "lost_partials_count")),
            "stranded_rows": series(ts, lambda t: a(t, "stranded_rows")),
            "stranded_never_restarted": series(
                ts, lambda t: a(t, "stranded_never_restarted")
            ),
            "reclaim_lag_median_s": series(ts, lambda t: a(t, "reclaim_lag_median_s")),
            "reclaim_lag_max_s": series(ts, lambda t: a(t, "reclaim_lag_max_s")),
            "unexpected_exits": series(ts, lambda t: len(t["unexpected_exits"])),
            "drain_elapsed_s": series(ts, lambda t: t["phases"].get("drain_elapsed_s")),
        }
    return summary


# --------------------------------------------------------------------------
# Orchestration.
# --------------------------------------------------------------------------


def harness_identity() -> dict:
    def git(*args: str) -> str | None:
        out = subprocess.run(
            ["git", "-C", str(BENCH_DIR), *args], capture_output=True, text=True
        )
        return out.stdout.strip() if out.returncode == 0 else None

    status = git("status", "--porcelain", "--untracked-files=all", "--", ".") or ""
    # Result files and logs are expected to be untracked while a run is in
    # progress; anything else listed here is harness code that differs from
    # the recorded commit.
    uncommitted = [
        line
        for line in status.splitlines()
        if not re.search(r"results-kill-.*\.json$|/logs/", line)
    ]
    return {
        "file": "benchmarks/killbench.py",
        "git_sha": git("rev-parse", "HEAD"),
        "git_branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "uncommitted_harness_changes": uncommitted,
    }


def versions() -> dict:
    import django_ox

    out = {"django-ox": django_ox.__version__}
    for name in ("Django", "django-tasks-db", "psycopg"):
        try:
            out[name] = version(name)
        except PackageNotFoundError:
            out[name] = None
    return out


def uptime() -> str:
    out = subprocess.run(["uptime"], capture_output=True, text=True)
    return out.stdout.strip()


def print_trial(record: dict) -> None:
    a = record["analysis"]
    print(
        f"  {record['label']}: statuses {a['status_counts']}, never terminal {a['never_terminal']}, "
        f"nonces {a['distinct_nonces']}, repeated effects {a['repeated_effects_count']}, "
        f"repeated starts {a['repeated_starts_count']}, lost partials {a['lost_partials_count']}"
    )
    print(
        f"  kills delivered {record['kills_delivered']}/{record['kills_planned']}, "
        f"windows {a['kill_windows']}, stranded {a['stranded_rows']}, "
        f"never restarted {a['stranded_never_restarted']}, "
        f"reclaim lag median {a['reclaim_lag_median_s']} max {a['reclaim_lag_max_s']} "
        f"(bound {a['reclaim_bound_s']}), unexpected exits {len(record['unexpected_exits'])}"
    )
    print(
        f"  kill offsets after last start (ms) {a['kill_ms_after_last_start']}, "
        f"body median {a['body_ms_median']} ms, inter-task gap median "
        f"{a['inter_task_gap_ms_median']} ms"
    )
    print(
        f"  exit codes {[w['exit_code'] for w in record['workers']]}, "
        f"last status change +{record['phases'].get('last_status_change_s')}s, "
        f"idle confirmed +{record['phases'].get('drain_elapsed_s')}s, "
        f"drained={record['drained']}"
    )
    for c in a["self_checks"]:
        mark = "PASS" if c["passed"] else "FAIL"
        print(f"    {mark}  {c['check']}: {c['observed']}")


def orchestrate(args: argparse.Namespace) -> None:
    smoke = args.smoke
    params = dict(SMOKE if smoke else FULL)
    windows = {
        "idle_seconds": args.idle_seconds,
        "observe_seconds": args.observe_seconds,
    }
    dev = windows != {"idle_seconds": IDLE_SECONDS, "observe_seconds": OBSERVE_SECONDS}
    arms = [args.arm] if args.arm else list(ARMS)
    seed = args.seed if args.seed is not None else random.randrange(1_000_000)

    bench.ensure_docker()
    bench.ensure_container()
    bench.ensure_databases()
    LOG_DIR.mkdir(exist_ok=True)
    for arm in arms:
        print(f"Migrating {arm}...")
        bench.migrate(arm)
        recreate_ledger(arm)

    try:
        environment = bench.collect_environment()
    except Exception as exc:
        environment = {"error": f"bench.collect_environment failed: {exc!r}"}
    results = {
        "date": datetime.date.today().isoformat(),
        "started_at": iso_now(),
        "invocation": " ".join(sys.argv),
        "smoke": smoke,
        "seed": seed,
        "harness": harness_identity(),
        "parameters": {
            **params,
            "task_sleep_s": TASK_SLEEP,
            "observer_tick_range_s": TICK_RANGE_S,
            "sample_interval_s": SAMPLE_INTERVAL,
            "idle_seconds": windows["idle_seconds"],
            "observe_seconds": windows["observe_seconds"],
            "stop_timeout_s": STOP_TIMEOUT,
            "order": "per trial: ox then tasksdb",
            "kill_gap_note": (
                "gaps are drawn once per trial from random.Random(seed * 1000 + trial), "
                "so both arms of a trial get the same planned schedule; the first kill "
                "waits for the first SUCCESSFUL row; kills stop once READY is 0"
            ),
            "restart_policy": (
                "no supervisor in either arm; the harness starts a replacement worker "
                "with the same command immediately after each kill"
            ),
            "ox": {
                "lock_timeout_s": LOCK_TIMEOUT,
                "lock_timeout_default_s": 300.0,
                "reap_interval_s": REAP_INTERVAL,
                "poll_interval_s": OX_POLL_INTERVAL,
                "max_attempts": 3,
                "reclaim_bound_s": RECLAIM_BOUND,
                "reclaim_bound_note": (
                    "a stranded row's lease expires at most LOCK_TIMEOUT after its last "
                    "renewal; the replacement worker's reaper runs on its poll loop every "
                    "reap interval; so a reclaim is expected within LOCK_TIMEOUT + reap "
                    "interval + poll interval of the kill, and the idle and observation "
                    "windows together exceed that bound"
                ),
            },
        },
        "arms": {
            arm: {
                "label": ARMS[arm]["label"],
                "settings": ARMS[arm]["settings"],
                "tasks_module": ARMS[arm]["tasks_module"],
                "database": ARMS[arm]["database"],
                "table": ARMS[arm]["table"],
                "worker_command": ["python", *worker_command(arm, "<worker-name>")[1:]],
            }
            for arm in arms
        },
        "environment": environment,
        "versions": versions(),
        "uptime_before": uptime(),
        "trials": [],
    }
    suffix = ("-SMOKE" if smoke else "") + ("-DEV" if dev else "")
    out_path = BENCH_DIR / f"results-kill-{results['date']}{suffix}.json"

    def checkpoint() -> None:
        results["summary"] = summarize(results["trials"])
        out_path.write_text(json.dumps(results, indent=2, default=str))

    print(
        f"seed {seed}; {params['trials']} trial(s) x {arms}; N={params['n_tasks']} K={params['kills']}"
    )
    print(f"uptime before: {results['uptime_before']}")
    failed_checks = 0
    for trial in range(1, params["trials"] + 1):
        for arm in arms:
            print(f"[trial {trial}] {ARMS[arm]['label']}: starting at {iso_now()}")
            record = run_trial(arm, trial, seed * 1000 + trial, params, windows, smoke)
            results["trials"].append(record)
            checkpoint()
            print_trial(record)
            failed_checks += sum(
                1 for c in record["analysis"]["self_checks"] if not c["passed"]
            )

    results["uptime_after"] = uptime()
    results["finished_at"] = iso_now()
    checkpoint()
    print(f"uptime after: {results['uptime_after']}")
    print(f"\nRaw results: {out_path}")
    if failed_checks:
        sys.exit(
            f"{failed_checks} self-check(s) FAILED; the harness, not the backend, needs a look"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--role", choices=["preload"])
    parser.add_argument("--arm", choices=list(ARMS))
    parser.add_argument("--count", type=int, default=0)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="N=100, K=2, one trial per arm; never for published numbers.",
    )
    parser.add_argument(
        "--idle-seconds",
        type=float,
        default=IDLE_SECONDS,
        help="Seconds without a status change that end the drain (default %(default)s).",
    )
    parser.add_argument(
        "--observe-seconds",
        type=float,
        default=OBSERVE_SECONDS,
        help="Seconds to keep observing after the drain is idle (default %(default)s).",
    )
    args = parser.parse_args()

    if args.role == "preload":
        if not args.arm or not args.count:
            sys.exit("--role preload requires --arm and --count")
        role_preload(args.arm, args.count)
        return
    orchestrate(args)


if __name__ == "__main__":
    main()
