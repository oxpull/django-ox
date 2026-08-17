#!/usr/bin/env python3
"""Check that a release is internally consistent before it becomes permanent.

A published release is immutable. PyPI will not let you edit the description or
the project URLs after upload, so anything wrong in the metadata is wrong until
the next version. That happened once: 0.1.0 shipped without the Documentation
URL because the URL was added to pyproject.toml after the upload, and its page
kept rendering a superseded README. The only fix was another release.

Checks:
  1. pyproject version, `django_ox.__version__` and the newest CHANGELOG entry
     all agree.
  2. The CHANGELOG has a link reference for that version.
  3. Every project URL is well formed and points at a host we actually control.
  4. The README declares no version other than the one being released.
  5. On a tag build, the tag matches the declared version.

Usage:
    python tools/check_release.py              # consistency only
    python tools/check_release.py --tag v0.1.1 # also assert the tag matches
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path
from urllib.parse import urlparse

REPO = Path(__file__).resolve().parent.parent

EXPECTED_URL_KEYS = {"Homepage", "Documentation", "Repository", "Changelog", "Issues"}
ALLOWED_HOSTS = {"github.com", "oxpull.github.io", "pypi.org"}


def fail(message: str) -> str:
    return f"error: {message}"


def read_pyproject_version() -> tuple[str, dict[str, str]]:
    data = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    return project["version"], project.get("urls", {})


def read_dunder_version() -> str | None:
    text = (REPO / "src" / "django_ox" / "__init__.py").read_text(encoding="utf-8")
    match = re.search(r"^__version__\s*=\s*[\"']([^\"']+)[\"']", text, re.M)
    return match.group(1) if match else None


def read_changelog() -> tuple[str | None, set[str]]:
    text = (REPO / "CHANGELOG.md").read_text(encoding="utf-8")
    headings = re.findall(r"^##\s*\[([^\]]+)\]", text, re.M)
    newest = next((h for h in headings if h.lower() != "unreleased"), None)
    refs = set(re.findall(r"^\[([^\]]+)\]:\s*https?://", text, re.M))
    return newest, refs


def check_urls(urls: dict[str, str]) -> list[str]:
    problems: list[str] = []
    missing = EXPECTED_URL_KEYS - urls.keys()
    if missing:
        problems.append(
            fail(
                f"pyproject [project.urls] is missing {sorted(missing)}. "
                "These render as the sidebar links on the package page and cannot "
                "be added after the release is published."
            )
        )
    for key, url in sorted(urls.items()):
        parsed = urlparse(url)
        if parsed.scheme != "https":
            problems.append(fail(f"project URL {key} is not https: {url}"))
        if parsed.hostname not in ALLOWED_HOSTS:
            problems.append(
                fail(f"project URL {key} points at an unexpected host: {url}")
            )
    return problems


def check_readme(version: str) -> list[str]:
    """The README is the package page. A stale version in it is a public error."""
    text = (REPO / "README.md").read_text(encoding="utf-8")
    problems: list[str] = []
    for match in re.finditer(r"django-ox[=~><]=\s*([0-9]+\.[0-9]+\.[0-9]+)", text):
        if match.group(1) != version:
            problems.append(
                fail(
                    f"README pins django-ox {match.group(1)} but this release is "
                    f"{version}"
                )
            )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="git tag being built, e.g. v0.1.1")
    args = parser.parse_args()

    problems: list[str] = []

    version, urls = read_pyproject_version()
    dunder = read_dunder_version()
    newest, refs = read_changelog()

    if dunder is None:
        problems.append(fail("could not find __version__ in src/django_ox/__init__.py"))
    elif dunder != version:
        problems.append(
            fail(f"__version__ is {dunder} but pyproject version is {version}")
        )

    if newest is None:
        problems.append(fail("CHANGELOG has no released version heading"))
    elif newest != version:
        problems.append(
            fail(
                f"the newest CHANGELOG entry is [{newest}] but this release is "
                f"{version}. Add the entry before tagging: the changelog is part "
                "of the published artifact."
            )
        )
    elif version not in refs:
        problems.append(
            fail(f"CHANGELOG has no link reference for [{version}] at the bottom")
        )

    problems.extend(check_urls(urls))
    problems.extend(check_readme(version))

    if args.tag:
        expected = f"v{version}"
        if args.tag != expected:
            problems.append(
                fail(f"tag {args.tag} does not match the declared version ({expected})")
            )

    for problem in problems:
        print(problem)

    if problems:
        print(f"\ncheck-release: {len(problems)} problem(s). A release is permanent.")
        return 1

    print(f"check-release: {version} is consistent, URLs complete, changelog ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
