"""docs/llms-full.txt is generated from the docs pages and committed.

A docs edit that skips the regeneration would publish a stale copy, so the
committed file is compared against a fresh build here.
"""

from pathlib import Path

from tools.build_llms_full import OUTPUT, build, nav_pages, page_url


def test_llms_full_is_current():
    assert OUTPUT.read_text() == build(), (
        "docs/llms-full.txt is stale; run python tools/build_llms_full.py"
    )


def test_every_nav_page_is_included():
    text = OUTPUT.read_text()
    pages = nav_pages()
    assert pages, "no nav entries parsed from mkdocs.yml"
    for _title, filename in pages:
        assert Path("docs", filename).exists()
        assert f"# Source: {page_url(filename)}\n" in text


def test_changelog_snippet_is_resolved():
    assert "--8<--" not in OUTPUT.read_text()
