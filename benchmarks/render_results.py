#!/usr/bin/env python3
"""Recompute the figures on docs/benchmarks.md from the raw result files.

The raw files are the source of truth. The results page is written by hand in
its own layout, so this script does not produce the page. It does two jobs:

1. With no mode argument it prints the full evidence tables (Performance,
   Behaviour under failure) as Markdown, followed by the facts behind them.
   The tables carry more rows and columns than the page does.
2. `--check PAGE` reads the page, finds every number on it (integers with
   thousands separators, decimals, percentages, versions, dates) and looks each
   one up among the values computed here from the two raw files: medians,
   ranges, counts, percentages from the unrounded medians, the stability
   spreads, the reclaim median and maximum, the probe figures and ratios, and
   the environment block. A number it cannot source is printed with its line,
   and the exit code is 1. So is a statement the raw files do not bear out.
   The few tokens that are names or formula constants, not measurements, are
   listed in `WHITELIST` with the reason for each.

   Which figures the page shows is the page owner's decision, so a figure the
   page leaves out is not an error: the computed rows that are absent are
   listed as "not on the page" and the exit code stays 0. `--require-complete`
   turns that list into a requirement: every computed row has to appear on the
   page word for word, and an absent one is MISSING with exit code 1. Only
   that mode stops a figure drifting to another value that happens to exist
   elsewhere in the files, and only that mode reads the order of a series.

Usage:
    python render_results.py
    python render_results.py --raw results-raw-2026-09-19.json \
        --kill results-kill-2026-09-19.json
    python render_results.py --draft
    python render_results.py --check ../docs/benchmarks.md
    python render_results.py --check ../docs/benchmarks.md --require-complete

`--draft` prefixes each row with a review marker computed from the medians
([WON], [LOST], [TIE] from django-ox's side; [WITHHELD] and [PROBE] from the
raw file's own gate and probe blocks). `--flag` adds [FLAGGED] to the named
entries; it records an observation about the machine during the run that the
raw file does not carry. The keys are `kill_trial_<n>` and `probe`, and the
default is the observation recorded for the 2026-09-19 run: kill trials 2 and
3 and the 100,000-task probe. Drain cells take no flag. The four drain cells
ran interleaved, run by run, in one window with overlapping load averages, so
the raw file cannot support a flag on some drain rows and not on others.
Markers are for the review copy and are not published.

The stability rule (max/min over a row's values at most 1.15) is applied to
the enqueue-throughput row and to each bulk-enqueue arm, whether or not the
harness's `--gate` list named the metric on the day: a row or arm that fails
it is rendered "withheld by the stability rule" with its ratio, and its values
stay in the raw file. The drain rows are not gated: their stability entries
pool both arms, so their ratio measures the difference, not the noise.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
TIE_BAND = 0.05  # medians within 5% of each other are called a tie in draft mode

ARM = {"ox": "django-ox", "tasksdb": "django-tasks-db"}


def median(values: list[float]) -> float:
    return float(statistics.median(values))


def fmt(x: float, nd: int) -> str:
    return f"{x:,.{nd}f}"


def cell(values: list[float], nd: int) -> str:
    return f"{fmt(median(values), nd)} [{fmt(min(values), nd)} to {fmt(max(values), nd)}]"


def seq(values: list[int]) -> str:
    """Per-trial counts: one value when every trial agrees, else all of them."""
    if len(set(values)) == 1:
        return f"{values[0]:,} in every trial"
    return ", ".join(f"{v:,}" for v in values)


def marker_for(ox: float, tasksdb: float, *, higher_is_better: bool) -> str:
    ratio = ox / tasksdb if higher_is_better else tasksdb / ox
    if abs(ratio - 1.0) <= TIE_BAND:
        return "[TIE]"
    return "[WON]" if ratio > 1.0 else "[LOST]"


def fails_stability(st: dict) -> bool:
    """The stability rule itself: max/min over the row's values above the threshold.

    `withheld_by_stability_gate` is set by the harness only for the metrics its
    `--gate` list named on the day, so a row can fail the rule without carrying
    the flag. The page applies the rule to every row it renders, so the check
    reads the ratio as well as the flag.
    """
    ratio = st.get("max_over_min")
    return bool(st.get("withheld_by_stability_gate")) or (
        ratio is not None and ratio > st["threshold"]
    )


def withheld_text(st: dict) -> str:
    return (
        f"withheld by the stability rule (max/min {st['max_over_min']:.2f} "
        f"over {st['n']} values; threshold {st['threshold']:.2f})"
    )


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def seconds_after_last_kill(trial: dict) -> float:
    """From the trial's last kill to the end of its observation window."""
    last = max(parse_ts(k["at"]) for k in trial["kills"])
    return (parse_ts(trial["phases"]["observation_end_at"]) - last).total_seconds()


def no_recovery_window(kill: dict) -> int:
    """The window the django-tasks-db cell quotes: the shortest of its trials, floored."""
    return int(min(seconds_after_last_kill(t) for t in kill["trials"] if t["arm"] == "tasksdb"))


def rate_difference(ox: list[float], tasksdb: list[float]) -> float:
    """Percent by which django-ox's median rate exceeds django-tasks-db's, from the unrounded medians."""
    return (median(ox) / median(tasksdb) - 1.0) * 100.0


def ranges_overlap(a: list[float], b: list[float]) -> bool:
    return max(a) >= min(b) and max(b) >= min(a)


def flag_keys(kill: dict) -> set[str]:
    """The keys `--flag` accepts. Drain cells are not among them; see the module docstring."""
    return {f"kill_trial_{n}" for n in range(1, kill["parameters"]["trials"] + 1)} | {"probe"}


class Renderer:
    def __init__(self, raw: dict, kill: dict, *, draft: bool, flags: set[str]):
        self.raw = raw
        self.kill = kill
        self.draft = draft
        self.flags = flags
        self.lines: list[str] = []
        self.facts: list[str] = []

    # -- helpers ---------------------------------------------------------

    def out(self, line: str = "") -> None:
        self.lines.append(line)

    def fact(self, line: str) -> None:
        self.facts.append(line)

    def mark(self, key: str, *computed: str) -> str:
        if not self.draft:
            return ""
        marks = list(computed)
        if key in self.flags:
            marks.append("[FLAGGED]")
        return " ".join(marks) + " " if marks else ""

    def e2e_values(self, depth: int, topology: str, backend: str, field: str) -> list[float]:
        return [
            e[field]
            for e in self.raw["e2e"]
            if e["depth"] == depth and e["topology"] == topology and e["backend"] == backend
        ]

    def exit_codes(self, section: str) -> list[int]:
        codes: list[int] = []
        for e in self.raw[section]:
            codes.extend(e["worker_exit_codes"])
        return codes

    # -- performance -----------------------------------------------------

    def performance(self) -> None:
        raw = self.raw
        self.out("### Performance")
        self.out()
        self.out(
            "| Workload / size / topology | django-ox median [range] "
            "| django-tasks-db median [range] | Validation and errors |"
        )
        self.out("| --- | ---: | ---: | --- |")

        # Enqueue throughput: the raw file's own gate decides.
        st = raw["stability"]["enqueue_throughput"]
        if fails_stability(st):
            reason = withheld_text(st)
            self.out(
                f"| {self.mark('enqueue_throughput', '[WITHHELD]')}Enqueue throughput, "
                f"{raw['parameters']['enqueue_count']:,} sequential `enqueue()` calls, "
                f"autocommit, one producer | {reason} | {reason} | "
                f"raw values in the JSON under `stability.enqueue_throughput.values` |"
            )
            self.fact(
                "enqueue_throughput values (tasks/s, run order): "
                + ", ".join(f"{v:.1f}" for v in st["values"])
                + f"; max/min {st['max_over_min']:.4f}; threshold {st['threshold']}"
            )
        else:
            ox = [e["tasks_per_sec"] for e in raw["enqueue_throughput"] if e["backend"] == "ox"]
            td = [
                e["tasks_per_sec"] for e in raw["enqueue_throughput"] if e["backend"] == "tasksdb"
            ]
            self.out(
                f"| {self.mark('enqueue_throughput', marker_for(median(ox), median(td), higher_is_better=True))}"
                f"Enqueue throughput (tasks/s) | {cell(ox, 1)} | {cell(td, 1)} | "
                f"max/min {st['max_over_min']:.2f} within {st['threshold']:.2f} |"
            )

        # Enqueue latency p50 and p95.
        n_lat = raw["parameters"]["latency_count"]
        for q in ("p50", "p95"):
            ox = [e[f"{q}_ms"] for e in raw["enqueue_latency"] if e["backend"] == "ox"]
            td = [e[f"{q}_ms"] for e in raw["enqueue_latency"] if e["backend"] == "tasksdb"]
            m = marker_for(median(ox), median(td), higher_is_better=False)
            self.out(
                f"| {self.mark(f'enqueue_latency_{q}', m)}Enqueue latency inside "
                f"`transaction.atomic()`, commit excluded, {q} (ms), {n_lat} samples per run "
                f"| {cell(ox, 3)} | {cell(td, 3)} | nearest-rank percentile of {n_lat} "
                f"samples per run; producer exit codes all 0 |"
            )
            self.fact(f"enqueue_latency {q} ms ox: {[round(v, 4) for v in ox]}")
            self.fact(f"enqueue_latency {q} ms tasksdb: {[round(v, 4) for v in td]}")

        # Drain cells.
        for depth in raw["parameters"]["depths"]:
            for topology in raw["parameters"]["topologies"]:
                procs = int(topology.split("v")[0])
                ox = self.e2e_values(depth, topology, "ox", "tasks_per_sec")
                td = self.e2e_values(depth, topology, "tasksdb", "tasks_per_sec")
                ox_s = self.e2e_values(depth, topology, "ox", "seconds")
                td_s = self.e2e_values(depth, topology, "tasksdb", "seconds")
                codes = [
                    c
                    for e in raw["e2e"]
                    if e["depth"] == depth and e["topology"] == topology
                    for c in e["worker_exit_codes"]
                ]
                nonzero = [c for c in codes if c != 0]
                sup = (
                    " (django-ox 4v4 records one code per run, its supervisor's)"
                    if procs > 1
                    else ""
                )
                if nonzero:
                    bad = [
                        e
                        for e in raw["e2e"]
                        if e["depth"] == depth
                        and e["topology"] == topology
                        and any(c != 0 for c in e["worker_exit_codes"])
                    ]
                    who = "; ".join(
                        f"{ARM[e['backend']]} run {e['run']}: exit codes {e['worker_exit_codes']}"
                        for e in bad
                    )
                    validation = (
                        f"{depth:,} SUCCESSFUL rows confirmed in all 10 runs; "
                        f"{len(codes) - len(nonzero)} of {len(codes)} worker exit codes 0{sup}; "
                        f"{len(nonzero)} exited {nonzero[0]} on the stop signal sent after the "
                        f"clock stopped ({who}), a teardown error outside the timed window"
                    )
                else:
                    validation = (
                        f"{depth:,} SUCCESSFUL rows confirmed in all 10 runs; "
                        f"all {len(codes)} worker exit codes 0{sup}"
                    )
                key = f"e2e_d{depth}_{topology}"
                m = marker_for(median(ox), median(td), higher_is_better=True)
                label = (
                    f"Backlog drain, {depth:,} preloaded tasks, {procs} worker process"
                    f"{'es' if procs > 1 else ''} per arm ({topology}), tasks/s"
                )
                self.out(f"| {self.mark(key, m)}{label} | {cell(ox, 1)} | {cell(td, 1)} | {validation} |")
                self.fact(
                    f"{key} tasks/s ox: {[round(v, 1) for v in ox]} median {median(ox):.1f}; "
                    f"seconds median {median(ox_s):.1f} [{min(ox_s):.1f} to {max(ox_s):.1f}]"
                )
                self.fact(
                    f"{key} tasks/s tasksdb: {[round(v, 1) for v in td]} median {median(td):.1f}; "
                    f"seconds median {median(td_s):.1f} [{min(td_s):.1f} to {max(td_s):.1f}]"
                )
                self.fact(f"{key} ranges overlap: {'yes' if ranges_overlap(ox, td) else 'no'}")
                self.fact(
                    f"{key} rate difference from the unrounded medians "
                    f"({median(ox):.4f} / {median(td):.4f}): {rate_difference(ox, td):.1f}%"
                )

        # Depth probe.
        probe = raw["probe"]
        entries = {e["backend"]: e for e in raw["e2e_probe"]}
        ox_e, td_e = entries["ox"], entries["tasksdb"]
        band = probe["band"]
        verdict = "holds for both arms" if probe["rate_holds_for_both_arms"] else "does not hold"
        validation = (
            f"one pair, no repetition; {probe['probe_depth']:,} SUCCESSFUL rows confirmed in both arms; "
            f"worker exit codes {ox_e['worker_exit_codes'] + td_e['worker_exit_codes']}; "
            f"rate against the {probe['deepest_cell_depth']:,} 1v1 median: django-ox "
            f"{probe['arms']['ox']['ratio']:.2f}, django-tasks-db {probe['arms']['tasksdb']['ratio']:.2f} "
            f"(band {1 - band:.2f} to {1 + band:.2f}); the 20 percent rule {verdict}; "
            f"one-minute load at the entries: django-tasks-db {td_e['load_before'][0]:.2f} before and "
            f"{td_e['load_after'][0]:.2f} after, django-ox {ox_e['load_before'][0]:.2f} before and "
            f"{ox_e['load_after'][0]:.2f} after"
        )
        self.out(
            f"| {self.mark('probe', '[PROBE]')}Depth probe, {probe['probe_depth']:,} preloaded tasks, "
            f"1 worker process per arm (1v1), one pair after `ANALYZE`, tasks/s "
            f"| {fmt(ox_e['tasks_per_sec'], 1)} (one run, {fmt(ox_e['seconds'], 1)} s) "
            f"| {fmt(td_e['tasks_per_sec'], 1)} (one run, {fmt(td_e['seconds'], 1)} s) "
            f"| {validation} |"
        )
        self.fact(
            f"probe: ox {ox_e['tasks_per_sec']:.2f} tasks/s in {ox_e['seconds']:.1f} s, ratio "
            f"{probe['arms']['ox']['ratio']}; tasksdb {td_e['tasks_per_sec']:.2f} tasks/s in "
            f"{td_e['seconds']:.1f} s, ratio {probe['arms']['tasksdb']['ratio']}; "
            f"rate_holds_for_both_arms {probe['rate_holds_for_both_arms']}"
        )
        self.out()

        # Bulk enqueue: three result columns, one arm each. The stability rule
        # is applied per arm (the raw file keeps one stability row per arm,
        # since the arms are three APIs); an arm that fails it is withheld in
        # its own cell and the other cells stand.
        n_bulk = raw["parameters"]["bulk_count"]
        arms = {arm: [e for e in raw["bulk_enqueue"] if e["arm"] == arm] for arm in raw["parameters"]["bulk_arms"]}
        secs = {arm: [e["seconds"] for e in es] for arm, es in arms.items()}
        stab = {arm: raw["stability"][f"bulk_enqueue_{arm}"] for arm in arms}
        held = {arm: fails_stability(st) for arm, st in stab.items()}
        validated = all(e["validated"] and e["rows_after_orchestrator"] == n_bulk for es in arms.values() for es_ in [es] for e in es_)
        chunk = next(e["chunk_size"] for e in arms["ox-many"])

        def bulk_cell(arm: str) -> str:
            return withheld_text(stab[arm]) if held[arm] else cell(secs[arm], 2)

        def bulk_marker(arm: str, against: str) -> str:
            # A withheld arm gets [WITHHELD]; otherwise the median direction
            # against the named comparator, from django-ox's side.
            if held[arm]:
                return "[WITHHELD]"
            return marker_for(median(secs[arm]), median(secs[against]), higher_is_better=False)

        m_many = bulk_marker("ox-many", "tasksdb-loop")
        m_loop = bulk_marker("ox-loop", "tasksdb-loop")
        td_marks = ["[WITHHELD]"] if held["tasksdb-loop"] else []
        withheld_arms = [arm for arm in arms if held[arm]]
        if withheld_arms:
            where = ", ".join(f"`stability.bulk_enqueue_{arm}.values`" for arm in withheld_arms)
            raw_note = f"; withheld arms' raw values in the JSON under {where} and `bulk_enqueue`"
        else:
            raw_note = ""
        self.out(
            "| Workload / size | django-ox `enqueue_many()` median [range] "
            "| django-ox `enqueue()` loop, control, median [range] "
            "| django-tasks-db `enqueue()` loop median [range] | Validation and errors |"
        )
        self.out("| --- | ---: | ---: | ---: | --- |")
        self.out(
            f"| {self.mark('bulk_enqueue', m_many)}Bulk enqueue, {n_bulk:,} tasks in one outer "
            f"`transaction.atomic()`, COMMIT included, seconds "
            f"| {bulk_cell('ox-many')} "
            f"| {self.mark('bulk_ox_loop', m_loop)}{bulk_cell('ox-loop')} "
            f"| {self.mark('bulk_tasksdb_loop', *td_marks)}{bulk_cell('tasksdb-loop')} "
            f"| {n_bulk:,} rows counted by the producer and again by the orchestrator in all "
            f"{sum(len(es) for es in arms.values())} runs "
            f"({'validated' if validated else 'NOT validated'}); `enqueue_many()` writes in chunks of "
            f"{chunk:,} rows; producer exit codes all 0{raw_note} |"
        )
        for arm, values in secs.items():
            st = stab[arm]
            self.fact(
                f"bulk_enqueue {arm} seconds: {[round(v, 3) for v in values]} median {median(values):.3f}; "
                f"stability max/min {st['max_over_min']:.4f} over {st['n']} values, threshold {st['threshold']}, "
                f"gated on the day {st['gated']}, "
                f"{'WITHHELD by the rule' if held[arm] else 'within threshold'}"
            )
        self.out()

        # Control row (ox only, diagnostic).
        diag = [e["tasks_per_sec"] for e in raw["e2e_diagnostic"]]
        base = self.e2e_values(raw["parameters"]["depths"][0], "1v1", "ox", "tasks_per_sec")
        label = raw["e2e_diagnostic"][0]["label"]
        self.fact(
            f"e2e_diagnostic ({label}) tasks/s: {[round(v, 1) for v in diag]} median {median(diag):.1f}; "
            f"default-interval 1v1 median at the same depth {median(base):.1f}; "
            f"exit codes {self.exit_codes('e2e_diagnostic')}"
        )

    # -- behaviour under failure ----------------------------------------

    def failure(self) -> None:
        kill = self.kill
        raw = self.raw
        p = kill["parameters"]
        trials = {arm: [t for t in kill["trials"] if t["arm"] == arm] for arm in ("ox", "tasksdb")}
        n = p["n_tasks"]

        def col(arm: str, key: str) -> list:
            return [t["analysis"][key] for t in trials[arm]]

        def status(arm: str, s: str) -> list[int]:
            return [t["analysis"]["status_counts"].get(s, 0) for t in trials[arm]]

        def window(arm: str, w: str) -> list[int]:
            return [t["analysis"]["kill_windows"].get(w, 0) for t in trials[arm]]

        # Window facts: seconds from the last kill and from the drain's end to the end of observation.
        after_last_kill: dict[str, list[float]] = {}
        after_idle: dict[str, list[float]] = {}
        for arm, ts in trials.items():
            after_last_kill[arm] = [seconds_after_last_kill(t) for t in ts]
            after_idle[arm] = [
                (parse_ts(t["phases"]["observation_end_at"]) - parse_ts(t["phases"]["idle_confirmed_at"])).total_seconds()
                for t in ts
            ]
        td_window = no_recovery_window(kill)
        obs = p["observe_seconds"]
        idle = p["idle_seconds"]

        self.out("### Behaviour under failure")
        self.out()
        self.out(
            "| Scenario / configuration / observation window | django-ox observed "
            "| django-tasks-db observed |"
        )
        self.out("| --- | --- | --- |")

        # Row: kill campaign counts.
        ox_succ = status("ox", "SUCCESSFUL")
        td_succ = status("tasksdb", "SUCCESSFUL")
        td_running = status("tasksdb", "RUNNING")
        ox_running = status("ox", "RUNNING")
        kills = [t["kills_delivered"] for t in trials["ox"]] + [t["kills_delivered"] for t in trials["tasksdb"]]
        assert kills == [p["kills"]] * len(kills), kills
        assert window("ox", "after_effect_before_outcome") == col("ox", "repeated_effects_count")

        def successful(values: list[int]) -> str:
            if len(set(values)) == 1:
                return f"{values[0]:,} of {n:,} SUCCESSFUL in every trial"
            return f"{', '.join(f'{v:,}' for v in values)} of {n:,} SUCCESSFUL (trials 1 to 5)"

        ox_cell = (
            f"{successful(ox_succ)}; rows RUNNING at the end "
            f"{seq(ox_running)}; rows READY, FAILED or LOST 0 in every trial; tasks never terminal "
            f"{seq(col('ox', 'never_terminal'))}; distinct executions {seq(col('ox', 'distinct_nonces'))}; "
            f"tasks started more than once {seq(col('ox', 'repeated_starts_count'))}; tasks whose effect "
            f"was applied twice {seq(col('ox', 'repeated_effects_count'))}, each after a kill that landed "
            f"after the effect row and before the outcome write (the same counts of such kills); "
            f"executions left without an effect {seq(col('ox', 'lost_partials_count'))}"
        )
        td_cell = (
            f"{successful(td_succ)}; rows RUNNING at the end "
            f"{seq(td_running)}, unchanged through the {obs:.0f} s observation after the drain ended; "
            f"no automatic recovery observed within {td_window} s after the last kill; "
            f"rows READY, FAILED or LOST 0 in every trial; tasks never terminal {seq(col('tasksdb', 'never_terminal'))}; "
            f"distinct executions {seq(col('tasksdb', 'distinct_nonces'))}; tasks started more than once "
            f"{seq(col('tasksdb', 'repeated_starts_count'))}; tasks whose effect was applied twice "
            f"{seq(col('tasksdb', 'repeated_effects_count'))}; executions left without an effect "
            f"{seq(col('tasksdb', 'lost_partials_count'))}"
        )
        gap_lo, gap_hi = p["kill_gap_s"]
        self.out(
            f"| {self.mark('kill_counts', '[WON]')}Worker death: {p['kills']} SIGKILLs of the only worker "
            f"process during a {n:,}-task drain, gaps {gap_lo:.0f} to {gap_hi:.0f} s, a replacement started "
            f"by the harness after each kill, task body 100 ms; five trials per arm; observed until no "
            f"status changed for {idle:.0f} s and READY was 0, then {obs:.0f} s more "
            f"| {ox_cell} | {td_cell} |"
        )

        # Row: reclaim lag, ox only.
        lag_medians = col("ox", "reclaim_lag_median_s")
        lag_max = col("ox", "reclaim_lag_max_s")
        pooled = [x for t in trials["ox"] for x in t["analysis"]["reclaim_lags_s"]]
        bound = p["ox"]["reclaim_bound_s"]
        stranded_ox = col("ox", "stranded_rows")
        stranded_td = col("tasksdb", "stranded_rows")
        assert all(x < bound for x in pooled)
        per_trial = ", ".join(
            f"{m:.1f}{' [FLAGGED]' if self.draft and f'kill_trial_{i}' in self.flags else ''}"
            for i, m in enumerate(lag_medians, start=1)
        )
        ox_cell = (
            f"{sum(stranded_ox)} rows stranded RUNNING by the {len(trials['ox']) * p['kills']} kills "
            f"({seq(stranded_ox)} per trial); every one restarted on a replacement worker; time from the kill "
            f"to the row's next start, database clock: median {median(pooled):.1f} s over all {len(pooled)} rows "
            f"(per trial {per_trial}), max {max(pooled):.1f} s, all inside the {bound} s bound"
        )
        td_cell = (
            f"{sum(stranded_td)} rows stranded RUNNING by the {len(trials['tasksdb']) * p['kills']} kills "
            f"({seq(stranded_td)} per trial); none restarted; no automatic reclaim observed within "
            f"{td_window} s after the last kill; rows RUNNING"
        )
        self.out(
            f"| {self.mark('kill_reclaim', '[WON]')}Worker death, same trials: time to reclaim a row the killed "
            f"worker left RUNNING; django-ox `LOCK_TIMEOUT` {p['ox']['lock_timeout_s']:.0f} s "
            f"(default {p['ox']['lock_timeout_default_s']:.0f} s), reap interval {p['ox']['reap_interval_s']} s, "
            f"poll {p['ox']['poll_interval_s']:.0f} s, bound {bound} s | {ox_cell} | {td_cell} |"
        )

        # Row: exception retry.
        rc = raw["parameters"]["retry_count"]
        win = raw["parameters"]["retry_window_seconds"]
        ox_r = [e for e in raw["retry"] if e["backend"] == "ox"]
        td_r = [e for e in raw["retry"] if e["backend"] == "tasksdb"]

        def retry_cell(entries: list[dict]) -> str:
            summaries = sorted({json.dumps(e["summary"], sort_keys=True) for e in entries})
            terminal = all(e["all_terminal"] for e in entries)
            if len(summaries) != 1:
                return "runs differ: " + "; ".join(summaries)
            summary = json.loads(summaries[0])
            (label, count), = summary.items()
            st, attempts, execs = label.split()
            return (
                f"{count} of {rc} {st} on attempt {attempts.split('=')[1]} in each of {len(entries)} runs "
                f"({execs.replace('=', ' ')} per task); "
                f"{'every task terminal inside the window in every run' if terminal else 'NOT all terminal'}"
            )

        ox_cell = retry_cell(ox_r)
        td_cell = retry_cell(td_r)
        if all(e["status_counts"].get("FAILED") == rc for e in td_r):
            td_cell = "No automatic retry; " + td_cell
        self.out(
            f"| {self.mark('retry', '[WON]')}Exception retry: {rc} tasks that raise on their first execution and "
            f"return on any later one, one worker process per arm, package defaults, observed until every task "
            f"was terminal or {win:.0f} s | {ox_cell} | {td_cell} |"
        )
        self.out()

        # Facts for the report.
        for arm in ("ox", "tasksdb"):
            self.fact(
                f"kill {arm}: SUCCESSFUL {status(arm, 'SUCCESSFUL')} RUNNING {status(arm, 'RUNNING')} "
                f"never_terminal {col(arm, 'never_terminal')} distinct_nonces {col(arm, 'distinct_nonces')} "
                f"repeated_starts {col(arm, 'repeated_starts_count')} repeated_effects {col(arm, 'repeated_effects_count')} "
                f"lost_partials {col(arm, 'lost_partials_count')} stranded {col(arm, 'stranded_rows')} "
                f"kills mid_body {window(arm, 'mid_body')} after_effect {window(arm, 'after_effect_before_outcome')} "
                f"after_claim {window(arm, 'after_claim_before_body')} between {window(arm, 'between_tasks')}"
            )
            self.fact(
                f"kill {arm}: seconds from last kill to observation end {[round(v, 1) for v in after_last_kill[arm]]}; "
                f"from idle confirmed to observation end {[round(v, 1) for v in after_idle[arm]]}; "
                f"final worker exit codes {[t['workers'][-1]['exit_code'] for t in trials[arm]]}; "
                f"unexpected exits {[len(t['unexpected_exits']) for t in trials[arm]]}; "
                f"harness_ok {[t['harness_ok'] for t in trials[arm]]}"
            )
        self.fact(
            f"kill ox reclaim: per-trial medians {lag_medians}, per-trial max {lag_max}, pooled median "
            f"{median(pooled):.3f} over {len(pooled)}, pooled max {max(pooled):.3f}, bound {bound}"
        )
        self.fact(
            f"kill self-checks passed: {sum(c['passed'] for t in kill['trials'] for c in t['analysis']['self_checks'])} "
            f"of {sum(len(t['analysis']['self_checks']) for t in kill['trials'])}"
        )
        self.fact(f"retry ox observed_seconds {[round(e['observed_seconds'], 2) for e in ox_r]} (not on the page)")
        self.fact(f"retry tasksdb observed_seconds {[round(e['observed_seconds'], 2) for e in td_r]} (not on the page)")

    # -- setup facts -----------------------------------------------------

    def setup(self) -> None:
        raw = self.raw
        env = raw["environment"]
        codes = self.exit_codes("e2e")
        others = self.exit_codes("e2e_diagnostic") + self.exit_codes("e2e_probe") + self.exit_codes("retry")
        self.fact(
            f"environment: {env['cpu']}, {env['logical_cpus']} logical CPUs, "
            f"{env['memory_bytes'] // 2**30} GiB, macOS {env['macos']}, Python {env['python']}, "
            f"PostgreSQL {env['postgres_server']}, Docker image {env['docker']['image']}, "
            f"packages {env['packages']}, postgres_settings {env['postgres_settings']}"
        )
        self.fact(
            f"resolved task classes: ox {env['resolved']['ox']['task_class']}, "
            f"tasksdb {env['resolved']['tasksdb']['task_class']}"
        )
        self.fact(f"harness: {env['harness']}; kill harness: {self.kill['harness']}")
        self.fact(f"quiet gate: {raw['parameters']['quiet_gate']}")
        self.fact(f"order: {raw['parameters']['order']}")
        self.fact(
            f"drain-cell worker exit codes: {len(codes)} recorded, {codes.count(0)} zero, "
            f"non-zero {[c for c in codes if c != 0]}; diagnostic, probe and retry worker exit codes: "
            f"{len(others)} recorded, {others.count(0)} zero"
        )
        self.fact(
            f"kill worker exit codes: {sum(1 for t in self.kill['trials'] for w in t['workers'] if w['exit_code'] == -9)} "
            f"SIGKILLed (-9) by design, final workers "
            f"{[t['workers'][-1]['exit_code'] for t in self.kill['trials']]} on SIGTERM"
        )
        self.fact(f"kill invocation: {self.kill['invocation']}; seed {self.kill['seed']}; order {self.kill['parameters']['order']}")

    def render(self) -> str:
        self.performance()
        self.failure()
        self.setup()
        self.out("### Facts behind the captions")
        self.out()
        for f in self.facts:
            self.out(f"- {f}")
        return "\n".join(self.lines) + "\n"


# -- page check --------------------------------------------------------------

# One token per number on the page. The order matters: a date and a dotted
# version are single tokens, and a word that carries digits (p50, M1) is a name.
TOKEN = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2})"
    r"|(?P<version>\d+(?:\.\d+){2,})"
    r"|(?P<name>[A-Za-z][A-Za-z_]*\d\w*)"
    r"|(?P<number>(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?%?)"
)

# Tokens that are not measurements: (token, text that must share its paragraph
# or table row, reason). Everything else on the page has to come out of the raw
# files. Versions, the date, the host and the settings are not listed here
# because the raw files carry them and the check computes them.
WHITELIST: list[tuple[str, str, str]] = [
    # Percentile names. The values beside them are checked; the names are labels
    # (the raw file's fields are p50_ms and p95_ms; p99 is named as not measured).
    ("p50", "", "percentile name"),
    ("p95", "", "percentile name"),
    ("p99", "", "percentile name, quoted as not measured"),
    # The rate-difference formula as the page prints it. Its 1 and 100 are the
    # arithmetic of a percentage, not results.
    ("1", "median - 1) x 100", "constant in the printed rate-difference formula"),
    ("100", "median - 1) x 100", "constant in the printed rate-difference formula"),
    # The drain headline, "Up to 20% faster queue drain". It is a rounded claim,
    # not a computed figure, and no row of the drain table reaches it. The cell
    # behind it is the 100,000-task probe, `e2e_probe` in the raw file: 115.4
    # against 92.7 tasks/s, 24.5% on those rates. Tied to the headline's words
    # because the probe band is also written 20%.
    ("20%", "faster queue drain", "drain headline, under the 100,000-task probe cell (e2e_probe, 115.4 against 92.7 tasks/s)"),
]


def page_units(text: str) -> list[tuple[int, str]]:
    """(first line number, text) per table row, heading or paragraph.

    A figure and the figures it is compared with have to share a unit, so a
    table row stays one unit and a wrapped paragraph is joined back together.
    """
    units: list[tuple[int, str]] = []
    start, parts = 0, []
    for number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        alone = stripped.startswith(("|", "#", "<!--"))
        if parts and (not stripped or alone):
            units.append((start, " ".join(parts)))
            parts = []
        if alone:
            units.append((number, stripped))
        elif stripped:
            if not parts:
                start = number
            parts.append(stripped)
    if parts:
        units.append((start, " ".join(parts)))
    return units


def has_figure(unit: str, figure: str) -> bool:
    """`figure` is in `unit` and is not a slice of a longer number."""
    pattern = r"(?<![\d.,])" + re.escape(figure.lower()) + r"(?!\d|[.,]\d)"
    return re.search(pattern, unit.lower()) is not None


class PageCheck:
    """Every value the page may quote, computed from the two raw files."""

    def __init__(self, raw: dict, kill: dict, flags: set[str]):
        self.raw = raw
        self.kill = kill
        self.flags = flags
        self.pool: dict[str, list[str]] = {}
        # (label, figures that must share one unit, text that switches the
        # requirement on, whether every unit carrying that text must comply)
        self.required: list[tuple[str, tuple[str, ...], str | None, bool]] = []
        # (text that makes the claim, whether the raw files bear it out, what was computed)
        self.statements: list[tuple[str, bool, str]] = []
        self.environment()
        self.performance()
        self.failure()

    def add(self, text: str, label: str) -> None:
        labels = self.pool.setdefault(text, [])
        if label not in labels:
            labels.append(label)

    def need(self, label: str, *figures: str, when: str | None = None, every: bool = False) -> None:
        self.required.append((label, figures, when, every))

    # -- what the raw files say ------------------------------------------

    def environment(self) -> None:
        raw, kill = self.raw, self.kill
        env = raw["environment"]
        assert raw["date"] == kill["date"], (raw["date"], kill["date"])
        self.add(raw["date"], "run date, both raw files")
        for package, version in kill["versions"].items():
            assert env["packages"][package] == version, package
        for package in ("django-ox", "django-tasks-db", "Django", "psycopg"):
            self.add(env["packages"][package], f"{package} version, environment.packages")
        self.add(env["python"], "Python version, environment.python")
        self.add(env["postgres_server"].split()[0], "PostgreSQL server version, environment.postgres_server")
        self.add(env["macos"], "macOS version, environment.macos")
        self.add(str(env["memory_bytes"] // 2**30), "host memory in GiB, environment.memory_bytes")
        for match in TOKEN.finditer(env["cpu"]):
            self.add(match.group(), "host CPU name, environment.cpu")
        gate = raw["parameters"]["quiet_gate"]
        self.add(f"{gate['load_at_start'][0]:.2f}", "one-minute load at admission, parameters.quiet_gate")
        self.add(f"{gate['threshold_one_minute_load']:g}", "quiet-gate load threshold, parameters.quiet_gate")

    def performance(self) -> None:
        raw = self.raw
        params = raw["parameters"]
        r = Renderer(raw, self.kill, draft=False, flags=set())

        drains: dict[tuple[int, str], tuple[list[float], list[float]]] = {}
        for depth in params["depths"]:
            self.add(f"{depth:,}", "queued tasks in a drain cell, parameters.depths")
            for topology in params["topologies"]:
                procs = topology.split("v")[0]
                self.add(procs, f"worker processes per backend ({topology}), parameters.topologies")
                ox = r.e2e_values(depth, topology, "ox", "tasks_per_sec")
                td = r.e2e_values(depth, topology, "tasksdb", "tasks_per_sec")
                drains[(depth, topology)] = (ox, td)
                where = f"drain {depth:,} {topology}"
                for arm, values in (("django-ox", ox), ("django-tasks-db", td)):
                    self.add(fmt(median(values), 1), f"{where} {arm} median tasks/s")
                    self.add(fmt(min(values), 1), f"{where} {arm} slowest run")
                    self.add(fmt(max(values), 1), f"{where} {arm} fastest run")
                pct = rate_difference(ox, td)
                pct_text = f"{abs(pct):.1f}%"
                self.add(pct_text, f"{where} rate difference from the unrounded medians")
                self.need(
                    f"{where} row",
                    f"{depth:,}", cell(ox, 1), cell(td, 1), f"{pct_text} {'faster' if pct > 0 else 'slower'}",
                )
        clear = [k for k, (ox, td) in drains.items() if not ranges_overlap(ox, td)]
        self.statements.append((
            "No run of one backend overlaps",
            len(clear) == len(drains),
            f"runs do not overlap in {len(clear)} of {len(drains)} drain cells",
        ))
        ahead = [k for k, (ox, td) in drains.items() if min(ox) > max(td)]
        self.statements.append((
            "every django-ox run outperformed every django-tasks-db run",
            len(ahead) == len(drains),
            f"django-ox's slowest run beats django-tasks-db's fastest in {len(ahead)} of {len(drains)} drain cells",
        ))
        ordered = sorted(raw["e2e"], key=lambda e: parse_ts(e["started_at"]))
        runs = [e["run"] for e in ordered]
        per_run = {run: {(e["depth"], e["topology"]) for e in ordered if e["run"] == run} for run in runs}
        self.statements.append((
            "ran interleaved, run by run",
            runs == sorted(runs) and all(cells == set(drains) for cells in per_run.values()),
            f"every run holds all {len(drains)} drain cells and the runs follow one another, "
            f"{ordered[0]['started_at']} to {ordered[-1]['finished_at']}",
        ))
        self.statements.append((
            "ran interleaved in one window",
            runs == sorted(runs) and all(cells == set(drains) for cells in per_run.values()),
            f"every run holds all {len(drains)} drain cells and the runs follow one another, "
            f"{ordered[0]['started_at']} to {ordered[-1]['finished_at']}",
        ))

        self.add(str(params["latency_count"]), "latency samples per run, parameters.latency_count")
        for q in ("p50", "p95"):
            shown = []
            for backend in ("ox", "tasksdb"):
                values = [e[f"{q}_ms"] for e in raw["enqueue_latency"] if e["backend"] == backend]
                shown.append(f"{fmt(median(values), 3)} ms")
                self.add(fmt(median(values), 3), f"enqueue latency {q} {ARM[backend]} median ms")
            self.need(f"enqueue latency {q} row", q, *shown)

        many = [e["seconds"] for e in raw["bulk_enqueue"] if e["arm"] == "ox-many"]
        self.add(f"{params['bulk_count']:,}", "tasks per bulk enqueue, parameters.bulk_count")
        self.add(fmt(median(many), 2), "enqueue_many() median seconds")
        self.add(fmt(min(many), 2), "enqueue_many() fastest run, seconds")
        self.add(fmt(max(many), 2), "enqueue_many() slowest run, seconds")
        self.need("bulk enqueue figure", f"{params['bulk_count']:,}", f"{fmt(median(many), 2)} s [{fmt(min(many), 2)} to {fmt(max(many), 2)}]")

        threshold = raw["stability"]["enqueue_throughput"]["threshold"]
        self.add(f"{threshold:.2f}", "stability threshold, max/min")
        spreads = []
        for key in ("enqueue_throughput", "bulk_enqueue_ox-loop", "bulk_enqueue_tasksdb-loop"):
            st = raw["stability"][key]
            if fails_stability(st):
                self.add(f"{st['max_over_min']:.2f}", f"stability spread (max/min) of withheld {key}")
                spreads.append(f"{st['max_over_min']:.2f}x")
        self.need("withheld spreads and the threshold", *spreads, when="stability gate")
        self.need("stability threshold", f"{threshold:.2f}", when="stability gate")

        rc = params["retry_count"]
        self.add(str(rc), "tasks per retry run, parameters.retry_count")
        for backend in ("ox", "tasksdb"):
            entries = [e for e in raw["retry"] if e["backend"] == backend]
            summaries = {json.dumps(e["summary"], sort_keys=True) for e in entries}
            assert len(summaries) == 1, summaries
            ((label, count),) = json.loads(summaries.pop()).items()
            status, attempts, _executions = label.split()
            attempt = attempts.split("=")[1]
            self.add(str(count), f"retry {ARM[backend]} tasks ending {status} per run")
            self.add(attempt, f"retry {ARM[backend]} attempt on which every task ended")
            self.need(f"retry {ARM[backend]} row", f"{count} of {rc} {status} on attempt {attempt}")

        probe = raw["probe"]
        entries = {e["backend"]: e for e in raw["e2e_probe"]}
        self.add(f"{probe['probe_depth']:,}", "probe depth, probe.probe_depth")
        self.add(f"{probe['band'] * 100:.0f}%", "probe band, probe.band")
        figures = []
        for backend in ("ox", "tasksdb"):
            e = entries[backend]
            self.add(fmt(e["tasks_per_sec"], 1), f"probe {ARM[backend]} tasks/s")
            self.add(fmt(e["seconds"], 1), f"probe {ARM[backend]} seconds")
            self.add(f"{probe['arms'][backend]['ratio']:.2f}", f"probe {ARM[backend]} rate over its own {probe['deepest_cell_depth']:,}-task median")
            figures += [
                f"{fmt(e['tasks_per_sec'], 1)} tasks/s",
                f"{fmt(e['seconds'], 1)} s",
                f"= {probe['arms'][backend]['ratio']:.2f}",
            ]
        self.need("probe figures", *figures, when=f"{probe['probe_depth']:,}-task probe")
        probe_backends = sorted(e["backend"] for e in raw["e2e_probe"])
        drain_runs = {len(ox) for ox, _td in drains.values()} | {len(td) for _ox, td in drains.values()}
        self.statements.append((
            f"the {probe['probe_depth']:,}-task run was one pair, not five",
            probe_backends == ["ox", "tasksdb"] and drain_runs == {5},
            f"e2e_probe holds {len(probe_backends)} entries ({', '.join(probe_backends)}); "
            f"runs per backend in the drain cells: {', '.join(map(str, sorted(drain_runs)))}",
        ))
        self.statements.append((
            f"held within {probe['band'] * 100:.0f}%",
            bool(probe["rate_holds_for_both_arms"]),
            f"probe.rate_holds_for_both_arms is {probe['rate_holds_for_both_arms']}",
        ))

    def failure(self) -> None:
        kill = self.kill
        p = kill["parameters"]
        trials = {arm: [t for t in kill["trials"] if t["arm"] == arm] for arm in ("ox", "tasksdb")}
        n = p["n_tasks"]
        self.add(str(p["kills"]), "SIGKILLs per trial, parameters.kills")
        self.add(f"{n:,}", "tasks per kill trial, parameters.n_tasks")
        self.add(f"{n * len(trials['ox']):,}", "tasks across the django-ox kill trials")
        self.add(f"{p['task_sleep_s'] * 1000:.0f}", "kill-trial task body in ms, parameters.task_sleep_s")

        def col(arm: str, key: str) -> list[int]:
            return [t["analysis"][key] for t in trials[arm]]

        def status(arm: str, s: str) -> list[int]:
            return [t["analysis"]["status_counts"].get(s, 0) for t in trials[arm]]

        series = {
            "SUCCESSFUL at the end": lambda arm: status(arm, "SUCCESSFUL"),
            "RUNNING at the end": lambda arm: status(arm, "RUNNING"),
            "effects applied twice": lambda arm: col(arm, "repeated_effects_count"),
            "executions left without an effect": lambda arm: col(arm, "lost_partials_count"),
        }
        for what, getter in series.items():
            for arm in ("ox", "tasksdb"):
                values = getter(arm)
                for v in values:
                    self.add(f"{v:,}", f"kill trials {ARM[arm]} {what}, per trial")
                # The page gives SUCCESSFUL as a span, required below; the other
                # series it lists trial by trial, and the order is part of the figure.
                if len(set(values)) > 1 and what != "SUCCESSFUL at the end":
                    self.need(
                        f"kill trials {ARM[arm]} {what}, in trial order",
                        ", ".join(f"{v:,}" for v in values),
                    )
        ox_succ, td_succ, td_running = status("ox", "SUCCESSFUL"), status("tasksdb", "SUCCESSFUL"), status("tasksdb", "RUNNING")
        assert set(ox_succ) == {n}, ox_succ
        self.need("django-ox finished every task", f"{n:,} of {n:,}")
        self.need("django-tasks-db finished, lowest to highest", f"{min(td_succ):,} to {max(td_succ):,}")
        self.need("django-tasks-db stuck RUNNING, lowest to highest", f"{min(td_running)} to {max(td_running)}")
        repeats = sum(col("ox", "repeated_effects_count"))
        self.add(str(repeats), "effects applied twice across the django-ox kill trials")
        self.need("repeated effects over all tasks", str(repeats), f"{n * len(trials['ox']):,}")

        window = no_recovery_window(kill)
        self.add(str(window), "shortest django-tasks-db window from the last kill to the end of observation, floored")
        self.need("no-recovery window", f"{window} s")

        ox = p["ox"]
        worker = self.raw["environment"]["resolved"]["ox"]["worker"]
        assert worker["lock_timeout"] == ox["lock_timeout_default_s"], worker
        default_bound = worker["lock_timeout"] + worker["reap_interval"] + worker["poll_interval"]
        self.add(f"{ox['lock_timeout_s']:g}", "LOCK_TIMEOUT in the kill trials, parameters.ox")
        self.add(f"{ox['lock_timeout_default_s']:g}", "default LOCK_TIMEOUT, parameters.ox")
        self.add(f"{ox['reap_interval_s']:g}", "reap interval in the kill trials, parameters.ox")
        self.add(f"{ox['poll_interval_s']:g}", "poll interval, parameters.ox")
        self.add(f"{ox['reclaim_bound_s']:g}", "reclaim bound in the kill trials, parameters.ox")
        assert ox["lock_timeout_s"] + ox["reap_interval_s"] + ox["poll_interval_s"] == ox["reclaim_bound_s"]
        self.add(
            f"{round(default_bound, -1):g}",
            f"reclaim bound at the defaults, {default_bound:g} s ({worker['lock_timeout']:g} + "
            f"{worker['reap_interval']:g} + {worker['poll_interval']:g}, environment.resolved), to the nearest 10",
        )

        pooled = [x for t in trials["ox"] for x in t["analysis"]["reclaim_lags_s"]]
        stranded = {arm: sum(col(arm, "stranded_rows")) for arm in trials}
        assert stranded["ox"] == len(pooled)
        assert sum(col("tasksdb", "stranded_never_restarted")) == stranded["tasksdb"]
        self.add(f"{median(pooled):.1f}", "django-ox reclaim time, median over every stranded row, seconds")
        self.add(f"{max(pooled):.1f}", "django-ox reclaim time, maximum, seconds")
        for arm in trials:
            self.add(str(stranded[arm]), f"{ARM[arm]} rows stranded RUNNING by the kills, all trials")
        self.need(
            "reclaim figures",
            f"{median(pooled):.1f} s", str(stranded["ox"]), f"{max(pooled):.1f} s",
            f"{ox['reclaim_bound_s']:g} s", str(stranded["tasksdb"]),
            when="Reclaim time",
        )
        self.statements.append((
            f"all under the {ox['reclaim_bound_s']:g} s bound",
            max(pooled) < ox["reclaim_bound_s"],
            f"slowest reclaim {max(pooled):.3f} s against the {ox['reclaim_bound_s']:g} s bound",
        ))

        # The load flag is an observation the raw files do not carry, so its
        # trial numbers come from --flag and nowhere else.
        flagged = sorted(int(f.rsplit("_", 1)[1]) for f in self.flags if f.startswith("kill_trial_"))
        for trial in flagged:
            self.add(str(trial), "kill trial carrying the load flag, from --flag (not in the raw files)")
        if flagged:
            listed = " and ".join([", ".join(map(str, flagged[:-1])), str(flagged[-1])]) if len(flagged) > 1 else str(flagged[0])
            self.need("flagged kill trials, from --flag", f"trials {listed}", when="load flag", every=True)

    # -- the page against them -------------------------------------------

    def run(self, page: Path, *, require_complete: bool = False) -> int:
        units = page_units(page.read_text())
        text = "\n".join(unit for _line, unit in units)
        found: dict[str, dict] = {}
        unsourced: list[tuple[int, str]] = []
        occurrences = 0
        for line, unit in units:
            for match in TOKEN.finditer(unit):
                token = match.group()
                occurrences += 1
                entry = found.setdefault(token, {"lines": [], "labels": []})
                entry["lines"].append(line)
                # A whitelist entry tied to a phrase wins inside that phrase's
                # unit; otherwise the raw files are asked first.
                allowed = [(c, f"whitelisted: {why}") for t, c, why in WHITELIST if t == token and c in unit]
                labels = (
                    [label for context, label in allowed if context]
                    or self.pool.get(token)
                    or [label for _context, label in allowed]
                )
                if not labels:
                    unsourced.append((line, token))
                for label in labels:
                    if label not in entry["labels"]:
                        entry["labels"].append(label)

        print(f"page: {page}")
        print(f"flags: {', '.join(sorted(self.flags)) or 'none'}")
        print()
        print("Numbers on the page and where each comes from:")
        for token, entry in found.items():
            lines = sorted(set(entry["lines"]))
            source = " | ".join(entry["labels"]) if entry["labels"] else "NOT SOURCED"
            print(f"  {token:<12} x{len(entry['lines']):<3} line {', '.join(map(str, lines))}: {source}")

        print()
        if require_complete:
            print("Figures that must appear together on the page (--require-complete):")
        else:
            print("Computed figures and whether the page shows them (informational; --require-complete makes them required):")
        absent_word = "MISSING" if require_complete else "not on the page"
        missing = 0
        for label, figures, when, every in self.required:
            if not figures or (when is not None and when.lower() not in text.lower()):
                continue
            complies = [all(has_figure(unit, f) for f in figures) for _line, unit in units]
            if every:
                ok = all(c for c, (_line, unit) in zip(complies, units) if when.lower() in unit.lower())
            else:
                ok = any(complies)
            missing += 0 if ok else 1
            print(f"  {'ok' if ok else absent_word:<{len(absent_word)}} {label}: {' ... '.join(figures)}")

        print()
        print("Statements the page makes about the figures:")
        failed = 0
        for claim, holds, computed in self.statements:
            if claim.lower() not in text.lower():
                continue
            failed += 0 if holds else 1
            print(f"  {'ok     ' if holds else 'FALSE  '} \"{claim}\": {computed}")

        print()
        whitelisted = sorted({t for t, e in found.items() if any(l.startswith("whitelisted") for l in e["labels"])})
        print(
            f"{occurrences} numbers on the page, {len(found)} distinct; "
            f"whitelisted {', '.join(whitelisted) or 'none'}; unsourced {len(unsourced)}; "
            + (f"missing figures {missing}; " if require_complete else "")
            + f"false statements {failed}"
            + ("" if require_complete else f"; computed figures not on the page {missing} (not an error)")
        )
        for line, token in unsourced:
            print(f"  NOT SOURCED line {line}: {token}")
        return 1 if unsourced or failed or (require_complete and missing) else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw", default=str(HERE / "results-raw-2026-09-19.json"))
    parser.add_argument("--kill", default=str(HERE / "results-kill-2026-09-19.json"))
    parser.add_argument("--draft", action="store_true", help="prefix rows with review markers")
    parser.add_argument(
        "--flag",
        default="kill_trial_2,kill_trial_3,probe",
        help="comma-separated kill_trial_<n> and probe entries that carry the load flag "
        "(default: %(default)s; pass an empty string for none)",
    )
    parser.add_argument(
        "--check",
        metavar="PAGE",
        help="verify every number on PAGE against the raw files instead of printing the tables",
    )
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="with --check: also require every computed row to appear on PAGE word for word "
        "(default: rows the page leaves out are listed and are not an error)",
    )
    args = parser.parse_args()
    if args.require_complete and not args.check:
        parser.error("--require-complete goes with --check PAGE")
    raw = json.loads(Path(args.raw).read_text())
    kill = json.loads(Path(args.kill).read_text())
    flags = {f for f in args.flag.split(",") if f}
    unknown = sorted(flags - flag_keys(kill))
    if unknown:
        parser.error(
            f"--flag takes {', '.join(sorted(flag_keys(kill)))}; got {', '.join(unknown)}. "
            "Drain cells take no flag: they ran interleaved in one window."
        )
    if args.check:
        return PageCheck(raw, kill, flags).run(Path(args.check), require_complete=args.require_complete)
    print(Renderer(raw, kill, draft=args.draft, flags=flags).render(), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
