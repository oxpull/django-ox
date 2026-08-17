"""Tests for the external-copy linter.

The cases in REAL_FAILURES are not invented. Each one is a sentence that
actually reached a public surface, or an internal marker that was actually
tracked in the public repository, and had to be removed by hand. Encoding them
here means loosening a pattern breaks a test instead of quietly re-opening the
hole it was written to close.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent


def _load_copy_lint():
    """Import tools/copy_lint.py, which is a script rather than a package."""
    spec = importlib.util.spec_from_file_location(
        "copy_lint", REPO / "tools" / "copy_lint.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["copy_lint"] = module
    spec.loader.exec_module(module)
    return module


copy_lint = _load_copy_lint()


# (text, expected rule id). Every one of these shipped or was tracked publicly.
REAL_FAILURES = [
    # docs/benchmarks.md, found by Philip 2026-08-17. The sentence that started
    # the copy standard.
    (
        (
            "django-ox's worker was measurably slow at launch shape, "
            "the benchmark caught it."
        ),
        "SELF_DOWNGRADE",
    ),
    (
        (
            "django-ox's worker was measurably slow at launch shape, "
            "the benchmark caught it."
        ),
        "BUG_AUTOBIOGRAPHY",
    ),
    # docs/benchmarks.md opening line, pre-sweep.
    ("The benchmark caught two real bugs.", "BUG_AUTOBIOGRAPHY"),
    # The live waitlist page, pre-sweep.
    (
        "All runs including the ones we lost before fixing a worker bug.",
        "BUG_AUTOBIOGRAPHY",
    ),
    # benchmarks/results-2026-08-13.md, pre-sweep.
    ("ox lost every cell in the first round.", "SELF_DOWNGRADE"),
    # CHANGELOG 0.1.0, pre-sweep.
    ("Fixed: findings from a pre-release adversarial review.", "BUG_AUTOBIOGRAPHY"),
    # RELEASE-CHECKLIST.md, tracked in the public repo.
    ("[PHILIP] approve before shipping.", "INTERNAL_LEAK"),
    ("See BUILD-NOTES.md for the full detail.", "INTERNAL_LEAK"),
    # Classes the standard bans that have not shipped, guarded pre-emptively.
    ("This module was generated with Claude.", "AI_DISCLOSURE"),
    ("A seamless, blazing-fast queue that will supercharge your stack.", "AI_SLOP"),
    ("In today's fast-paced world, background jobs matter.", "AI_SLOP"),
    ("It is robust and battle-tested, truly best-in-class.", "AI_SLOP"),
    ("Django-OX is a task queue.", "BRAND"),
    ("Buy oxpull Pro today.", "BRAND"),
    ("TODO: write this section.", "INTERNAL_LEAK"),
]


def _rule_ids(text: str, tmp_path: Path) -> set[str]:
    target = tmp_path / "sample.md"
    target.write_text(text, encoding="utf-8")
    return {f.rule.rule_id for f in copy_lint.lint_file(target)}


@pytest.mark.parametrize(("text", "rule_id"), REAL_FAILURES)
def test_known_failures_are_caught(text, rule_id, tmp_path):
    assert rule_id in _rule_ids(text, tmp_path), (
        f"{rule_id} no longer fires on: {text!r}"
    )


# Sentences that must NOT trip the linter. A gate that cries wolf gets disabled,
# so the false-positive side is tested as deliberately as the true-positive one.
LEGITIMATE = [
    "Enqueue is a single INSERT on your default connection.",
    "No-op benchmarks flatter every queue, so read these as relative.",
    "Retries use exponential backoff and keep every traceback.",
    "The methodology and the raw per-sample data ship in the repository.",
    "Outside the current scope: task revocation after enqueue.",
    "django-ox stores tasks in your existing database.",
    "Oxpull Pro adds batching, unique tasks and rate limiting.",
    "Install with `pip install django-ox` and add `django_ox` to INSTALLED_APPS.",
    "A reaper returns tasks whose worker died to the queue.",
    "Tasks should be idempotent, because execution is at-least-once.",
]


@pytest.mark.parametrize("text", LEGITIMATE)
def test_legitimate_copy_passes(text, tmp_path):
    errors = {
        f.rule.rule_id
        for f in copy_lint.lint_file(_written(tmp_path, text))
        if f.rule.error
    }
    assert not errors, f"false positive {errors} on: {text!r}"


def _written(tmp_path: Path, text: str) -> Path:
    target = tmp_path / "sample.md"
    target.write_text(text, encoding="utf-8")
    return target


def test_code_blocks_are_not_prose(tmp_path):
    """Identifiers and shell output are held to a different standard."""
    text = "```\ndjango_ox is slow\nDjango-OX\n```\n"
    assert not copy_lint.lint_file(_written(tmp_path, text))


def test_inline_code_is_not_prose(tmp_path):
    text = "Add `django_ox` to INSTALLED_APPS.\n"
    assert not copy_lint.lint_file(_written(tmp_path, text))


def test_suppression_requires_a_reason(tmp_path):
    with_reason = (
        "The worker is slow. "
        "<!-- copy-lint: allow SELF_DOWNGRADE the API is theirs not ours -->"
    )
    bare = "The worker is slow. <!-- copy-lint: allow SELF_DOWNGRADE -->"
    assert not copy_lint.lint_file(_written(tmp_path, with_reason))
    assert copy_lint.lint_file(_written(tmp_path, bare))


def test_suppression_is_scoped_to_its_rule(tmp_path):
    text = "The worker is slow. <!-- copy-lint: allow AI_SLOP not the right rule -->"
    assert "SELF_DOWNGRADE" in _rule_ids(text, tmp_path)


def test_disagreeing_test_counts_are_reported(tmp_path):
    one = _written(tmp_path, "The suite is 191 tests.")
    two = tmp_path / "other.md"
    two.write_text("The suite is 141 tests.", encoding="utf-8")
    assert copy_lint.check_test_count([one, two])


def test_agreeing_test_counts_are_quiet(tmp_path):
    one = _written(tmp_path, "The suite is 191 tests.")
    two = tmp_path / "other.md"
    two.write_text("All 191 tests pass on PostgreSQL.", encoding="utf-8")
    assert not copy_lint.check_test_count([one, two])


def test_the_published_corpus_is_clean():
    """The real target set must pass, or the gate is not actually enforced."""
    paths = copy_lint.collect([])
    assert paths, "the copy-lint target set matched no files"
    errors = [f for path in paths for f in copy_lint.lint_file(path) if f.rule.error]
    assert not errors, "\n".join(f.render() for f in errors)
    assert not copy_lint.check_test_count(paths)
