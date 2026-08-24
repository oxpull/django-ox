#!/usr/bin/env python3
"""Build docs/llms-full.txt: every docs page in nav order, in one file.

The file is committed so the site can serve it as a static file, and a test
checks it matches what this script produces, so a docs change that forgets to
regenerate it fails CI instead of publishing a stale copy.

Usage:
    python tools/build_llms_full.py          # rewrite docs/llms-full.txt
    python tools/build_llms_full.py --check  # exit 1 if it is out of date
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / "docs"
OUTPUT = DOCS / "llms-full.txt"
SITE_URL = "https://oxpull.com/django-ox/"

# mkdocs.yml uses a `!relative` tag that yaml.safe_load rejects, and the nav is
# a flat list, so a line match is simpler and has no dependency.
NAV_ENTRY = re.compile(r"^\s+- (?P<title>[^:]+): (?P<file>\S+\.md)\s*$")
SNIPPET = re.compile(r'^--8<-- "(?P<file>[^"]+)"\s*$')


def nav_pages() -> list[tuple[str, str]]:
    """(title, filename) for each nav entry, in order."""
    pages: list[tuple[str, str]] = []
    in_nav = False
    for line in (REPO / "mkdocs.yml").read_text().splitlines():
        if line.startswith("nav:"):
            in_nav = True
            continue
        if in_nav:
            if line and not line.startswith(" "):
                break
            match = NAV_ENTRY.match(line)
            if match:
                pages.append((match["title"], match["file"]))
    return pages


def page_url(filename: str) -> str:
    stem = filename.removesuffix(".md")
    return SITE_URL if stem == "index" else f"{SITE_URL}{stem}/"


def page_text(filename: str) -> str:
    """The page's markdown with snippet includes resolved."""
    lines: list[str] = []
    for line in (DOCS / filename).read_text().splitlines():
        match = SNIPPET.match(line)
        if match:
            lines.append((REPO / match["file"]).read_text().rstrip("\n"))
        else:
            lines.append(line)
    return "\n".join(lines).rstrip("\n")


def build() -> str:
    intro = (
        f"Every page of {SITE_URL} in navigation order. "
        f"The short index is at {SITE_URL}llms.txt.\n"
    )
    parts = ["# django-ox documentation\n", intro]
    for _title, filename in nav_pages():
        header = f"# Source: {page_url(filename)}"
        parts.append(f"\n---\n\n{header}\n\n{page_text(filename)}\n")
    return "".join(parts)


def main(argv: list[str]) -> int:
    text = build()
    if "--check" in argv:
        current = OUTPUT.read_text() if OUTPUT.exists() else ""
        if current != text:
            name = OUTPUT.relative_to(REPO)
            print(f"{name} is out of date; run {Path(__file__).name}")
            return 1
        return 0
    OUTPUT.write_text(text)
    print(f"wrote {OUTPUT.relative_to(REPO)} ({len(text)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
