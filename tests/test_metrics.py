"""
The Prometheus exposition over django_ox.stats.

The metric names and labels are public API once shipped, so the expected
output here is spelled out line by line. A rename has to change this
file on purpose.
"""

import ast
import importlib
import re
from pathlib import Path

import pytest
from django.urls import reverse

from django_ox import metrics
from django_ox.models import OxTask

from .test_stats import make_task

FAMILY_HEADERS = [
    "# HELP django_ox_tasks Rows in the task table by queue and status.",
    "# TYPE django_ox_tasks gauge",
    (
        "# HELP django_ox_ready_tasks READY tasks eligible to run now "
        "(run_after unset or passed)."
    ),
    "# TYPE django_ox_ready_tasks gauge",
    (
        "# HELP django_ox_oldest_ready_age_seconds Seconds the oldest eligible "
        "task has waited since becoming eligible."
    ),
    "# TYPE django_ox_oldest_ready_age_seconds gauge",
    (
        "# HELP django_ox_last_claim_age_seconds Seconds since a worker last "
        "claimed a task on the queue."
    ),
    "# TYPE django_ox_last_claim_age_seconds gauge",
    (
        "# HELP django_ox_throughput_per_minute Tasks reaching SUCCESSFUL or "
        "FAILED per minute over the trailing window."
    ),
    "# TYPE django_ox_throughput_per_minute gauge",
    (
        "# HELP django_ox_failure_rate Fraction of terminal outcomes in the "
        "trailing window that FAILED."
    ),
    "# TYPE django_ox_failure_rate gauge",
]


def without_ages(text):
    """The text with the clock-dependent age samples dropped, for comparisons."""
    return "\n".join(
        line
        for line in text.splitlines()
        if not re.match(r"django_ox_\w+_age_seconds\{", line)
    )


def seed():
    """One row of every status on `default`, plus a two-row `emails` queue."""
    make_task(OxTask.Status.READY, enqueued_minutes_ago=10)
    make_task(OxTask.Status.READY, run_after_minutes=60)  # deferred: not eligible
    make_task(OxTask.Status.RUNNING, last_attempted_minutes_ago=1)
    make_task(OxTask.Status.FAILED, finished_minutes_ago=1)
    make_task(OxTask.Status.SUCCESSFUL, finished_minutes_ago=1)
    make_task(OxTask.Status.SUCCESSFUL, finished_minutes_ago=1)
    make_task(OxTask.Status.SUCCESSFUL, finished_minutes_ago=1)
    make_task(OxTask.Status.LOST)
    make_task(OxTask.Status.DISCARDED)
    make_task(OxTask.Status.SUCCESSFUL, queue="emails", finished_minutes_ago=30)
    make_task(OxTask.Status.RUNNING, queue="emails")


@pytest.mark.django_db
class TestRenderPrometheus:
    def test_empty_table_renders_the_headers_only(self):
        text = metrics.render_prometheus()
        assert text == "\n".join(FAMILY_HEADERS) + "\n"

    def test_seeded_table(self):
        seed()
        lines = metrics.render_prometheus().splitlines()
        assert lines[:2] == FAMILY_HEADERS[:2]
        assert lines[2:14] == [
            'django_ox_tasks{queue="default",status="ready"} 2',
            'django_ox_tasks{queue="default",status="running"} 1',
            'django_ox_tasks{queue="default",status="failed"} 1',
            'django_ox_tasks{queue="default",status="successful"} 3',
            'django_ox_tasks{queue="default",status="lost"} 1',
            'django_ox_tasks{queue="default",status="discarded"} 1',
            'django_ox_tasks{queue="emails",status="ready"} 0',
            'django_ox_tasks{queue="emails",status="running"} 1',
            'django_ox_tasks{queue="emails",status="failed"} 0',
            'django_ox_tasks{queue="emails",status="successful"} 1',
            'django_ox_tasks{queue="emails",status="lost"} 0',
            'django_ox_tasks{queue="emails",status="discarded"} 0',
        ]
        assert lines[14:18] == [
            *FAMILY_HEADERS[2:4],
            'django_ox_ready_tasks{queue="default"} 1',
            'django_ox_ready_tasks{queue="emails"} 0',
        ]
        # The ages depend on the clock, so their shape is pinned, not their value.
        assert lines[18:21][:2] == FAMILY_HEADERS[4:6]
        assert re.fullmatch(
            r'django_ox_oldest_ready_age_seconds\{queue="default"\} 6\d\d(\.\d+)?',
            lines[20],
        )
        assert lines[21:24][:2] == FAMILY_HEADERS[6:8]
        assert re.fullmatch(
            r'django_ox_last_claim_age_seconds\{queue="default"\} 6\d(\.\d+)?',
            lines[23],
        )
        # emails has no eligible task and no claim, so it has no age samples.
        assert lines[24:28] == [
            *FAMILY_HEADERS[8:10],
            'django_ox_throughput_per_minute{queue="default"} 0.8',
            'django_ox_throughput_per_minute{queue="emails"} 0',
        ]
        assert lines[28:] == [
            *FAMILY_HEADERS[10:12],
            'django_ox_failure_rate{queue="default"} 0.25',
        ]

    def test_metric_names_are_pinned(self):
        assert metrics.METRIC_NAMES == (
            "django_ox_tasks",
            "django_ox_ready_tasks",
            "django_ox_oldest_ready_age_seconds",
            "django_ox_last_claim_age_seconds",
            "django_ox_throughput_per_minute",
            "django_ox_failure_rate",
        )
        assert [family.name for family in metrics.collect()] == list(
            metrics.METRIC_NAMES
        )

    def test_labels_are_pinned(self):
        seed()
        for family in metrics.collect():
            expected = (
                {"queue", "status"} if family.name == "django_ox_tasks" else {"queue"}
            )
            assert all(set(labels) == expected for labels, _ in family.samples)

    def test_queue_names_are_escaped(self):
        make_task(queue='we"ird\\name\nhere')
        line = metrics.render_prometheus().splitlines()[2]
        assert (
            line == 'django_ox_tasks{queue="we\\"ird\\\\name\\nhere",status="ready"} 1'
        )

    def test_openmetrics_adds_eof(self):
        seed()
        rendered = metrics.render_openmetrics()
        assert rendered.endswith("\n# EOF\n")
        assert without_ages(rendered) == without_ages(
            metrics.render_prometheus() + "# EOF\n"
        )

    def test_values_format_as_prometheus_expects(self):
        assert metrics._format_value(3.0) == "3"
        assert metrics._format_value(0.25) == "0.25"
        assert metrics._format_value(float("nan")) == "NaN"
        assert metrics._format_value(float("inf")) == "+Inf"


@pytest.mark.django_db
class TestView:
    def test_mounted_under_include(self, client):
        assert reverse("django_ox:metrics") == "/ox/metrics"

    def test_content_type_and_body(self, client):
        seed()
        response = client.get("/ox/metrics")
        assert response.status_code == 200
        assert response["Content-Type"] == "text/plain; version=0.0.4; charset=utf-8"
        assert without_ages(response.content.decode()) == without_ages(
            metrics.render_prometheus()
        )

    def test_openmetrics_on_accept(self, client):
        response = client.get(
            "/ox/metrics", HTTP_ACCEPT="application/openmetrics-text; version=1.0.0"
        )
        assert response["Content-Type"] == (
            "application/openmetrics-text; version=1.0.0; charset=utf-8"
        )
        assert response.content.decode().endswith("# EOF\n")

    def test_rejects_post(self, client):
        assert client.post("/ox/metrics").status_code == 405

    def test_has_no_auth_of_its_own(self, client):
        assert client.get("/ox/metrics").status_code == 200


@pytest.mark.django_db
class TestCollector:
    def test_import_error_names_the_package(self, monkeypatch):
        def refuse(name):
            raise ImportError(name)

        monkeypatch.setattr(importlib, "import_module", refuse)
        with pytest.raises(ImportError, match="prometheus_client"):
            metrics.collector()

    def test_registers_with_prometheus_client(self):
        pytest.importorskip("prometheus_client")
        from prometheus_client import CollectorRegistry, generate_latest

        seed()
        registry = CollectorRegistry()
        registry.register(metrics.collector())
        text = generate_latest(registry).decode()
        assert 'django_ox_tasks{queue="default",status="successful"} 3.0' in text
        assert 'django_ox_failure_rate{queue="default"} 0.25' in text
        assert 'django_ox_oldest_ready_age_seconds{queue="emails"}' not in text


OTEL_SNIPPET = re.compile(r"```python\n(?P<code>from opentelemetry[^`]*)```")


def test_otel_example_in_the_docs_parses():
    """The OpenTelemetry recipe is documentation only; check it is valid Python."""
    page = Path(__file__).parent.parent / "docs" / "monitoring.md"
    snippets = OTEL_SNIPPET.findall(page.read_text())
    assert len(snippets) == 1
    ast.parse(snippets[0])
    assert "queue_stats()" in snippets[0]
