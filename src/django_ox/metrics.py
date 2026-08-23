"""
Prometheus exposition of the queue metrics in django_ox.stats.

render_prometheus() turns the stats functions into the Prometheus text
format (version 0.0.4) with no dependency beyond the standard library.
collector() wraps the same numbers as a prometheus_client Collector for
projects that already run a registry. Both read the task table and
nothing else; there is no counter state held in the process.

Every number here is a gauge. The task table is pruned, so a monotonic
counter of finished tasks cannot be derived from it; throughput and the
failure rate are trailing-window readings over stats.DEFAULT_WINDOW.
"""

from __future__ import annotations

import importlib
import math
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from django.utils import timezone

from . import stats

__all__ = [
    "CONTENT_TYPE_OPENMETRICS",
    "CONTENT_TYPE_PROMETHEUS",
    "METRIC_NAMES",
    "MetricFamily",
    "collect",
    "collector",
    "render_openmetrics",
    "render_prometheus",
]

CONTENT_TYPE_PROMETHEUS = "text/plain; version=0.0.4; charset=utf-8"
CONTENT_TYPE_OPENMETRICS = "application/openmetrics-text; version=1.0.0; charset=utf-8"

STATUSES = ("ready", "running", "failed", "successful", "lost", "discarded")

METRIC_NAMES = (
    "django_ox_tasks",
    "django_ox_ready_tasks",
    "django_ox_oldest_ready_age_seconds",
    "django_ox_last_claim_age_seconds",
    "django_ox_throughput_per_minute",
    "django_ox_failure_rate",
)

_HELP = {
    "django_ox_tasks": "Rows in the task table by queue and status.",
    "django_ox_ready_tasks": (
        "READY tasks eligible to run now (run_after unset or passed)."
    ),
    "django_ox_oldest_ready_age_seconds": (
        "Seconds the oldest eligible task has waited since becoming eligible."
    ),
    "django_ox_last_claim_age_seconds": (
        "Seconds since a worker last claimed a task on the queue."
    ),
    "django_ox_throughput_per_minute": (
        "Tasks reaching SUCCESSFUL or FAILED per minute over the trailing window."
    ),
    "django_ox_failure_rate": (
        "Fraction of terminal outcomes in the trailing window that FAILED."
    ),
}


@dataclass(frozen=True)
class MetricFamily:
    """One gauge family: its name, help text and (labels, value) samples."""

    name: str
    help: str
    samples: tuple[tuple[dict[str, str], float], ...]


def collect(window: timedelta = stats.DEFAULT_WINDOW) -> list[MetricFamily]:
    """
    Every metric family, one sample per queue, in METRIC_NAMES order.

    A queue appears once it has any row. A metric with nothing to measure
    (no eligible task, no claim yet, nothing finished in the window) has
    no sample for that queue rather than a placeholder value.

    Five aggregate queries, however many queues there are: the per-status
    counts, then one grouped query for each reading those counts cannot
    give (eligible backlog, oldest wait, last claim, the finished window).
    """
    now = timezone.now()
    rows = stats.queue_stats()
    ready_by_queue = stats._ready_counts(now)
    oldest_by_queue = stats._oldest_ready(now)
    claims_by_queue = stats._last_claims()
    finished_by_queue = stats._finished_counts(window)
    minutes = window.total_seconds() / 60.0
    tasks: list[tuple[dict[str, str], float]] = []
    ready: list[tuple[dict[str, str], float]] = []
    oldest: list[tuple[dict[str, str], float]] = []
    claim: list[tuple[dict[str, str], float]] = []
    throughput: list[tuple[dict[str, str], float]] = []
    failure: list[tuple[dict[str, str], float]] = []
    for row in rows:
        queue = {"queue": row.queue_name}
        tasks.extend(
            ({"queue": row.queue_name, "status": status}, float(getattr(row, status)))
            for status in STATUSES
        )
        ready.append((queue, float(ready_by_queue.get(row.queue_name, 0))))
        eligible_since = oldest_by_queue.get(row.queue_name)
        if eligible_since is not None:
            oldest.append((queue, (now - eligible_since).total_seconds()))
        last_claim = claims_by_queue.get(row.queue_name)
        if last_claim is not None:
            claim.append((queue, (now - last_claim).total_seconds()))
        finished, failed = finished_by_queue.get(row.queue_name, (0, 0))
        throughput.append((queue, finished / minutes))
        if finished:
            failure.append((queue, failed / finished))
    samples = (tasks, ready, oldest, claim, throughput, failure)
    return [
        MetricFamily(name, _HELP[name], tuple(values))
        for name, values in zip(METRIC_NAMES, samples, strict=True)
    ]


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _format_value(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if value == int(value):
        return str(int(value))
    return repr(value)


def _lines(window: timedelta) -> Iterator[str]:
    for family in collect(window):
        yield f"# HELP {family.name} {family.help}"
        yield f"# TYPE {family.name} gauge"
        for labels, value in family.samples:
            rendered = ",".join(
                f'{key}="{_escape_label(text)}"' for key, text in labels.items()
            )
            yield f"{family.name}{{{rendered}}} {_format_value(value)}"


def render_prometheus(window: timedelta = stats.DEFAULT_WINDOW) -> str:
    """The metrics in the Prometheus text exposition format, version 0.0.4."""
    return "\n".join(_lines(window)) + "\n"


def render_openmetrics(window: timedelta = stats.DEFAULT_WINDOW) -> str:
    """
    The same metrics in OpenMetrics 1.0 text format.

    Gauges render identically in the two formats; OpenMetrics adds a
    terminating # EOF line, which is the whole difference here.
    """
    return render_prometheus(window) + "# EOF\n"


def _label_names(name: str) -> list[str]:
    return ["queue", "status"] if name == "django_ox_tasks" else ["queue"]


class _OxCollector:
    """A prometheus_client Collector over collect(). Built by collector()."""

    def __init__(self, family_class: Any, window: timedelta) -> None:
        self._family_class = family_class
        self._window = window

    def describe(self) -> Iterator[Any]:
        # Lets a registry register the collector without querying the
        # database, and tells it the names and labels so duplicates are
        # rejected and readers of the declaration see what collect() renders.
        for name in METRIC_NAMES:
            yield self._family_class(name, _HELP[name], labels=_label_names(name))

    def collect(self) -> Iterator[Any]:
        for family in collect(self._window):
            label_names = _label_names(family.name)
            metric = self._family_class(family.name, family.help, labels=label_names)
            for labels, value in family.samples:
                metric.add_metric([labels[key] for key in label_names], value)
            yield metric


def collector(window: timedelta = stats.DEFAULT_WINDOW) -> Any:
    """
    A Collector for a prometheus_client registry, when that package is
    installed. django-ox does not depend on it; the import happens here.

        from prometheus_client import REGISTRY
        REGISTRY.register(django_ox.metrics.collector())

    Raises ImportError when prometheus_client is not installed.
    """
    try:
        core = importlib.import_module("prometheus_client.core")
    except ImportError as exc:
        raise ImportError(
            "django_ox.metrics.collector() needs prometheus_client, which is not "
            "installed. render_prometheus() needs no extra package."
        ) from exc
    return _OxCollector(core.GaugeMetricFamily, window)
