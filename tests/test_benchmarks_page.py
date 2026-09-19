"""Every number on docs/benchmarks.md comes out of the committed raw files.

The page is written by hand, so nothing regenerates it. What holds it to the
data is `benchmarks/render_results.py --check`, run here the way a person runs
it: a figure the two raw JSON files do not produce fails the suite, and so does
a statement they do not bear out. Which figures the page shows is the owner's
decision, so a computed figure the page leaves out is listed and is not a
failure; `--require-complete` makes it one.
"""

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PAGE = REPO / "docs" / "benchmarks.md"
SCRIPT = REPO / "benchmarks" / "render_results.py"
RAW = REPO / "benchmarks" / "results-raw-2026-09-19.json"

OUTPERFORMED = "every django-ox run outperformed every django-tasks-db run"
RUNNING_SERIES = "kill trials django-tasks-db RUNNING at the end, in trial order"


def render_results(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def check(page: Path) -> subprocess.CompletedProcess[str]:
    return render_results("--check", str(page))


def test_every_number_on_the_page_is_sourced():
    result = check(PAGE)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "unsourced 0; false statements 0" in result.stdout
    assert "MISSING" not in result.stdout


def test_the_statements_on_the_page_are_read():
    """A zero for false statements means nothing unless the claims were found."""
    result = check(PAGE)
    for claim in (
        OUTPERFORMED,
        "ran interleaved in one window",
        "the 100,000-task run was one pair, not five",
    ):
        assert f'ok      "{claim}"' in result.stdout, claim


def test_a_changed_figure_is_reported(tmp_path):
    text = PAGE.read_text()
    assert text.count("118.5") == 1
    page = tmp_path / "benchmarks.md"
    page.write_text(text.replace("118.5", "119.5"))
    result = check(page)
    assert result.returncode == 1
    assert "NOT SOURCED line" in result.stdout
    assert "119.5" in result.stdout


def test_a_false_statement_is_reported(tmp_path):
    """One django-tasks-db run made the fastest of its cell: the medians, and so
    every number on the page, stay as they are, and the claim stops being true."""
    raw = json.loads(RAW.read_text())
    cell = [
        e
        for e in raw["e2e"]
        if (e["depth"], e["topology"], e["backend"]) == (2000, "1v1", "tasksdb")
    ]
    max(cell, key=lambda e: e["tasks_per_sec"])["tasks_per_sec"] = 500.0
    changed = tmp_path / "raw.json"
    changed.write_text(json.dumps(raw))
    result = render_results("--raw", str(changed), "--check", str(PAGE))
    assert result.returncode == 1, result.stdout + result.stderr
    assert f'FALSE   "{OUTPERFORMED}"' in result.stdout
    assert "unsourced 0; false statements 1" in result.stdout


def test_figures_the_page_leaves_out_are_listed_and_are_not_an_error():
    result = check(PAGE)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "not on the page no-recovery window: 291 s" in result.stdout
    assert f"not on the page {RUNNING_SERIES}" in result.stdout
    assert "(not an error)" in result.stdout


def test_require_complete_reports_the_absent_figures():
    result = render_results("--check", str(PAGE), "--require-complete")
    assert result.returncode == 1
    assert "unsourced 0; missing figures 14; false statements 0" in result.stdout
    assert "MISSING no-recovery window: 291 s" in result.stdout
    assert f"MISSING {RUNNING_SERIES}" in result.stdout
    assert "not on the page" not in result.stdout


# The page gives the stuck tasks as a span, so no series is on it to reorder.
# The order is read only under --require-complete; this holds that to a line
# of the kind the long page carried.
SERIES = "django-tasks-db left these RUNNING, by trial: {}.\n"


def test_a_reordered_trial_series_is_reported(tmp_path):
    page = tmp_path / "benchmarks.md"

    page.write_text(PAGE.read_text() + "\n" + SERIES.format("19, 16, 13, 15, 18"))
    in_order = render_results("--check", str(page), "--require-complete")
    assert f"ok      {RUNNING_SERIES}" in in_order.stdout, in_order.stdout

    page.write_text(PAGE.read_text() + "\n" + SERIES.format("19, 16, 13, 18, 15"))
    reordered = render_results("--check", str(page), "--require-complete")
    assert reordered.returncode == 1
    assert f"MISSING {RUNNING_SERIES}" in reordered.stdout
    # Every value is still one the raw file holds, so the default cannot see it.
    assert "unsourced 0" in reordered.stdout
    assert check(page).returncode == 0


def test_require_complete_goes_with_check():
    result = render_results("--require-complete")
    assert result.returncode == 2
    assert "--require-complete goes with --check" in result.stderr


def test_drain_cells_take_no_flag():
    result = render_results("--draft", "--flag", "e2e_d20000_1v1")
    assert result.returncode == 2
    assert "Drain cells take no flag" in result.stderr
