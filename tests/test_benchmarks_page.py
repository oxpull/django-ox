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
MATCHED = REPO / "benchmarks" / "results-raw-2026-10-14-matched.json"

EVERY_TASK = (
    "Every task finished in these runs, with zero duplicate starts or completions"
)
RUNNING_SERIES = "kill trials django-tasks-db RUNNING at the end, in trial order"
CPU = (
    "worker CPU per task was 1.0 to 1.4 ms for django-ox "
    "versus 1.5 to 2.1 ms for django-tasks-db"
)


def render_results(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def check(page: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return render_results("--check", str(page), *args)


def edited_page(tmp_path, old: str, new: str, *, count: int = 1) -> Path:
    text = PAGE.read_text()
    assert text.count(old) == count, (old, text.count(old))
    page = tmp_path / "benchmarks.md"
    page.write_text(text.replace(old, new))
    return page


def edited_matched(tmp_path, change) -> Path:
    data = json.loads(MATCHED.read_text())
    change(data["rows"])
    path = tmp_path / "matched.json"
    path.write_text(json.dumps(data))
    return path


def test_every_number_on_the_page_is_sourced():
    result = check(PAGE)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "unsourced 0; false statements 0" in result.stdout
    assert "MISSING" not in result.stdout


def test_the_statements_on_the_page_are_read():
    """A zero for false statements means nothing unless the claims were found."""
    result = check(PAGE)
    for claim in (
        EVERY_TASK,
        "five interleaved runs",
        "order balanced across rounds",
        "1.4 to 1.7\u00d7 the throughput",
        "across 4, 8 and 16 workers",
        CPU,
        "spread between runs is 9% on both",
        "about 550 new connections a second",
        "no default-settings throughput above 4 workers",
        "load average 1.9 to 6.8 before a run",
        "Draining 100,000 tasks at 16 workers took about 30 s",
    ):
        assert f'ok      "{claim}' in result.stdout, claim
    rows = [
        line
        for line in result.stdout.splitlines()
        if '"line ' in line and "tasks, " in line
    ]
    assert len(rows) == 17, rows
    assert all(line.strip().startswith("ok") for line in rows)


def test_the_retired_drain_section_is_gone():
    text = PAGE.read_text()
    for gone in ("118.5", "Up to 20%", "up to 20%", "20% faster", "OUTPERFORMED"):
        assert gone not in text, gone


def test_a_retired_figure_is_not_sourced(tmp_path):
    page = tmp_path / "benchmarks.md"
    page.write_text(PAGE.read_text() + "\nThe 1.3.0 drain ran at 118.5 tasks/s.\n")
    result = check(page)
    assert result.returncode == 1
    assert "NOT SOURCED" in result.stdout
    assert "118.5" in result.stdout


def test_a_changed_matched_figure_is_reported(tmp_path):
    result = check(edited_page(tmp_path, "1,771", "1,781"))
    assert result.returncode == 1
    assert "NOT SOURCED line" in result.stdout
    assert "1,781" in result.stdout


def test_a_changed_1_3_0_figure_is_reported(tmp_path):
    result = check(edited_page(tmp_path, "0.71 s", "0.77 s"))
    assert result.returncode == 1
    assert "NOT SOURCED line" in result.stdout
    assert "0.77" in result.stdout


def test_sourced_figures_in_the_wrong_row_are_reported(tmp_path):
    """Both numbers exist in the data, so only the row check can see the swap."""
    page = edited_page(tmp_path, "| 1,771 | 1,211 |", "| 1,211 | 1,771 |")
    result = check(page)
    assert result.returncode == 1, result.stdout
    assert "unsourced 0; false statements 1" in result.stdout
    assert "FALSE" in result.stdout
    assert "expected 1,771 | 1,211 | 1.46" in result.stdout


def test_a_false_sentence_with_sourced_numbers_is_reported(tmp_path):
    page = edited_page(tmp_path, "1.0 to 1.4 ms", "1.0 to 1.5 ms")
    result = check(page)
    assert result.returncode == 1
    assert f'FALSE   "{CPU.replace("1.4", "1.5")}' in result.stdout
    assert "unsourced 0; false statements 1" in result.stdout


def test_a_duplicate_start_makes_every_task_finished_false(tmp_path):
    """One repeated start in a shown row; every number on the page stays."""

    def change(rows):
        row = next(
            r
            for r in rows
            if (r["cell"], r["setting"], r["backend"], r["run"])
            == ("S1", "P", "django-ox", 1)
        )
        row["duplicate_starts"] = 1

    result = render_results(
        "--matched", str(edited_matched(tmp_path, change)), "--check", str(PAGE)
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert f'FALSE   "{EVERY_TASK}' in result.stdout
    assert "unsourced 0; false statements 1" in result.stdout


def test_a_task_left_running_makes_every_task_finished_false(tmp_path):
    def change(rows):
        row = next(
            r
            for r in rows
            if (r["cell"], r["setting"], r["backend"], r["run"])
            == ("S3", "G", "django-tasks-db", 2)
        )
        row["after_successful"] -= 1

    result = render_results(
        "--matched", str(edited_matched(tmp_path, change)), "--check", str(PAGE)
    )
    assert result.returncode == 1
    assert f'FALSE   "{EVERY_TASK}' in result.stdout


def test_a_worker_exit_code_makes_every_task_finished_false(tmp_path):
    def change(rows):
        row = next(
            r
            for r in rows
            if (r["cell"], r["setting"], r["backend"], r["run"])
            == ("S2.W8", "PH", "django-ox", 3)
        )
        row["worker_exit_codes"] = [1]

    result = render_results(
        "--matched", str(edited_matched(tmp_path, change)), "--check", str(PAGE)
    )
    assert result.returncode == 1
    assert f'FALSE   "{EVERY_TASK}' in result.stdout


def test_default_settings_throughput_above_4_workers_is_never_on_the_page(tmp_path):
    rows = json.loads(MATCHED.read_text())["rows"]
    for r in rows:
        if r["setting"] == "D" and r["workers"] > 4 and r["tasks_per_sec"]:
            assert f"{round(r['tasks_per_sec']):,}" not in PAGE.read_text()
    page = tmp_path / "benchmarks.md"
    page.write_text(
        PAGE.read_text()
        + "\n| 20,000 tasks, 8 workers | Defaults | 691 | 0 | **n/a** |\n"
    )
    result = check(page)
    assert result.returncode == 1
    assert "not one the page may show a throughput for" in result.stdout


def test_a_claim_about_default_settings_above_4_workers_is_checked(tmp_path):
    """Had those runs finished cleanly, the sentence hiding them would be false."""

    def change(rows):
        for r in rows:
            if r["setting"] == "D" and r["workers"] > 4:
                r.update(
                    status="ok",
                    completed=r["tasks"],
                    after_successful=r["tasks"],
                    duplicate_starts=0,
                    duplicate_dones=0,
                    worker_exit_codes=[0],
                )

    result = render_results(
        "--matched", str(edited_matched(tmp_path, change)), "--check", str(PAGE)
    )
    assert result.returncode == 1
    assert 'FALSE   "no default-settings throughput above 4 workers"' in result.stdout


def test_a_changed_headline_range_is_reported(tmp_path):
    """1.5 is a number the page already holds; only the claim check sees it is wrong."""
    result = check(
        edited_page(tmp_path, "delivered 1.4 to 1.7", "delivered 1.5 to 1.7")
    )
    assert result.returncode == 1
    assert 'FALSE   "1.5 to 1.7\u00d7 django-tasks-db' in result.stdout
    assert "unsourced 0; false statements 1" in result.stdout


def test_a_changed_spread_claim_is_reported(tmp_path):
    result = check(
        edited_page(tmp_path, "spread between runs is 9%", "spread between runs is 4%")
    )
    assert result.returncode == 1
    assert 'FALSE   "spread between runs is 4% on both"' in result.stdout


def test_figures_the_page_leaves_out_are_listed_and_are_not_an_error():
    result = check(PAGE)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "not on the page no-recovery window: 291 s" in result.stdout
    assert f"not on the page {RUNNING_SERIES}" in result.stdout
    assert "(not an error)" in result.stdout


def test_require_complete_reports_the_absent_figures():
    result = check(PAGE, "--require-complete")
    assert result.returncode == 1
    assert "unsourced 0; missing figures 10; false statements 0" in result.stdout
    assert "MISSING no-recovery window: 291 s" in result.stdout
    assert f"MISSING {RUNNING_SERIES}" in result.stdout
    assert "MISSING matched" not in result.stdout
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
