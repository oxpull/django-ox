#!/usr/bin/env python3
"""Check that a release is internally consistent before it becomes permanent.

A published release is immutable. PyPI will not let you edit the description or
the project URLs after upload, so anything wrong in the metadata is wrong until
the next version. Metadata added to pyproject.toml after an upload does not
reach the published page, so the URLs and the description are checked here,
before the upload rather than after it.

The source tree (`check_source`):
  1. pyproject version, `django_ox.__version__` and the newest CHANGELOG entry
     all agree.
  2. The CHANGELOG has a link reference for that version.
  3. Every project URL is well formed and points at a host we actually control.
  4. The README and the docs pages pin no django-ox version other than the
     one being released.
  5. On a tag build, the tag matches the declared version.

The built archives (`check_dist`), which the source tree cannot show:
  6. A wheel and an sdist exist at the declared version, and no other version
     is staged beside them.
  7. Both carry every migration in the source tree. A packaging rule that drops
     one is invisible until somebody upgrades and their table lacks a column,
     and no test that imports from the source tree can see it.
  8. Both carry the licence and the worker module.

The documentation site (`check_docs`), which publishes on its own schedule:
  9. The changelog has nothing unreleased. The site is one unversioned site
     deployed by hand, so a deploy from a tree ahead of PyPI would describe
     behaviour nobody can install.

Usage:
    python tools/check_release.py               # source only
    python tools/check_release.py --tag v0.1.1  # also assert the tag matches
    python tools/check_release.py --dist        # also open dist/
    python tools/check_release.py --docs        # before mkdocs gh-deploy
"""

from __future__ import annotations

import argparse
import re
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path
from urllib.parse import urlparse

REPO = Path(__file__).resolve().parent.parent

EXPECTED_URL_KEYS = {"Homepage", "Documentation", "Repository", "Changelog", "Issues"}
# The hosts a project URL may point at. This is an allowlist on purpose: a
# release is permanent, so a URL that drifts to an unexpected host has to
# fail here rather than ship. oxpull.com became the canonical site on
# 2026-08-19; oxpull.github.io stays because it still serves and redirects.
ALLOWED_HOSTS = {"github.com", "oxpull.com", "oxpull.github.io", "pypi.org"}


def fail(message: str) -> str:
    return f"error: {message}"


def read_pyproject_version(repo: Path = REPO) -> tuple[str, dict[str, str]]:
    data = tomllib.loads((repo / "pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    return project["version"], project.get("urls", {})


def read_dunder_version(repo: Path = REPO) -> str | None:
    text = (repo / "src" / "django_ox" / "__init__.py").read_text(encoding="utf-8")
    match = re.search(r"^__version__\s*=\s*[\"']([^\"']+)[\"']", text, re.M)
    return match.group(1) if match else None


def read_changelog(repo: Path = REPO) -> tuple[str | None, set[str]]:
    text = (repo / "CHANGELOG.md").read_text(encoding="utf-8")
    headings = re.findall(r"^##\s*\[([^\]]+)\]", text, re.M)
    newest = next((h for h in headings if h.lower() != "unreleased"), None)
    refs = set(re.findall(r"^\[([^\]]+)\]:\s*https?://", text, re.M))
    return newest, refs


def read_unreleased(repo: Path = REPO) -> str:
    """The body of the CHANGELOG's Unreleased section, empty when there is none."""
    text = (repo / "CHANGELOG.md").read_text(encoding="utf-8")
    match = re.search(
        r"^##\s*\[unreleased\][^\n]*\n(.*?)(?=^##\s|\Z)", text, re.M | re.S | re.I
    )
    return match.group(1).strip() if match else ""


def read_migrations(repo: Path = REPO) -> set[str]:
    """The migration filenames the source tree declares."""
    directory = repo / "src" / "django_ox" / "migrations"
    if not directory.is_dir():
        return set()
    return {
        path.name for path in directory.glob("[0-9]*.py") if path.name != "__init__.py"
    }


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


def check_readme(version: str, repo: Path = REPO) -> list[str]:
    """The README is the package page and the docs ship pin examples. A stale
    django-ox version on any of them is a public error."""
    problems: list[str] = []
    pages = [repo / "README.md", *sorted((repo / "docs").glob("*.md"))]
    for page in pages:
        text = page.read_text(encoding="utf-8")
        for match in re.finditer(r"django-ox[=~><]=\s*([0-9]+\.[0-9]+\.[0-9]+)", text):
            if match.group(1) != version:
                problems.append(
                    fail(
                        f"{page.name} pins django-ox {match.group(1)} but this "
                        f"release is {version}"
                    )
                )
    return problems


def check_docs(repo: Path = REPO) -> tuple[list[str], list[str]]:
    """The docs site is one unversioned site and deploys by hand, so what it
    describes has to be what the released package does. Returns (problems,
    notes)."""
    version, _ = read_pyproject_version(repo)
    unreleased = read_unreleased(repo)
    if unreleased:
        entries = [
            line.strip() for line in unreleased.splitlines() if line.startswith("- ")
        ]
        count = len(entries) or 1
        return [
            fail(
                f"CHANGELOG has {count} unreleased entr{'y' if count == 1 else 'ies'} "
                f"and the declared version is {version}. Deploying the docs now "
                "would describe behaviour that is not in the package on PyPI. "
                "Release first, then deploy from the release commit."
            )
        ], []
    return [], [f"CHANGELOG has nothing unreleased; the docs match {version}"]


def check_source(
    repo: Path = REPO, tag: str | None = None
) -> tuple[list[str], list[str]]:
    """Everything decidable from the tree. Returns (problems, notes)."""
    problems: list[str] = []
    notes: list[str] = []

    version, urls = read_pyproject_version(repo)
    dunder = read_dunder_version(repo)
    newest, refs = read_changelog(repo)

    if dunder is None:
        problems.append(fail("could not find __version__ in src/django_ox/__init__.py"))
    elif dunder != version:
        problems.append(
            fail(f"__version__ is {dunder} but pyproject version is {version}")
        )
    else:
        notes.append(f"__version__ and pyproject agree on {version}")

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
    else:
        notes.append(f"CHANGELOG has the [{version}] entry and its link reference")

    problems.extend(check_urls(urls))
    problems.extend(check_readme(version, repo))

    if tag:
        expected = f"v{version}"
        if tag != expected:
            problems.append(
                fail(f"tag {tag} does not match the declared version ({expected})")
            )
        else:
            notes.append(f"tag {tag} matches the declared version")

    return problems, notes


def _archive_members(path: Path) -> list[str] | None:
    """Member names inside a wheel or an sdist, or None if unreadable."""
    try:
        if path.suffix == ".whl":
            with zipfile.ZipFile(path) as archive:
                return archive.namelist()
        with tarfile.open(path, "r:gz") as archive:
            return archive.getnames()
    except (zipfile.BadZipFile, tarfile.TarError, OSError):
        return None


def check_dist(
    dist: Path, version: str, migrations: set[str] | None = None
) -> tuple[list[str], list[str]]:
    """Everything only the built archive can show. Returns (problems, notes)."""
    problems: list[str] = []
    notes: list[str] = []

    if not dist.is_dir():
        return [fail(f"{dist} does not exist; run python -m build first")], notes

    expected = read_migrations() if migrations is None else migrations

    for kind, pattern in (
        ("wheel", "django_ox-*.whl"),
        ("sdist", "django_ox-*.tar.gz"),
    ):
        staged = sorted(dist.glob(pattern))
        wanted = [p for p in staged if f"-{version}" in p.name.replace("_", "-")]
        others = [
            p
            for p in staged
            if p not in wanted and re.search(r"-([0-9]+\.[0-9]+\.[0-9]+)", p.name)
        ]
        if others:
            found = re.search(r"-([0-9]+\.[0-9]+\.[0-9]+)", others[0].name)
            seen = found.group(1) if found else "another version"
            problems.append(
                fail(
                    f"{dist} holds a django_ox {kind} at {seen} as well as "
                    f"{version}; nothing says which one would be uploaded"
                )
            )
        if not wanted:
            problems.append(fail(f"{dist} has no django_ox {kind} at {version}"))
            continue

        for path in wanted:
            members = _archive_members(path)
            if members is None:
                problems.append(fail(f"{path.name} could not be opened"))
                continue
            notes.append(f"inspected {path.name}")
            tails = {name.split("django_ox/", 1)[-1] for name in members}

            for required in ("__init__.py", "worker.py", "models.py"):
                if required not in tails:
                    problems.append(
                        fail(f"{path.name} does not contain django_ox/{required}")
                    )
            if not any(name.endswith("LICENSE") for name in members):
                problems.append(fail(f"{path.name} does not carry LICENSE"))

            shipped = {
                tail.split("migrations/", 1)[-1]
                for tail in tails
                if tail.startswith("migrations/") and tail != "migrations/__init__.py"
            }
            for missing in sorted(expected - shipped):
                problems.append(
                    fail(
                        f"{path.name} is missing migration {missing}. An upgrade "
                        "against it leaves the table without the column the code "
                        "expects, and nothing before install can see that."
                    )
                )

    return problems, notes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="git tag being built, e.g. v0.1.1")
    parser.add_argument(
        "--dist", action="store_true", help="also open the archives in dist/"
    )
    parser.add_argument(
        "--docs",
        action="store_true",
        help="also assert the tree is releasable, for a docs deploy",
    )
    args = parser.parse_args()

    version, _ = read_pyproject_version()
    problems, _ = check_source(REPO, tag=args.tag)
    if args.dist:
        dist_problems, _ = check_dist(REPO / "dist", version)
        problems.extend(dist_problems)
    if args.docs:
        docs_problems, _ = check_docs(REPO)
        problems.extend(docs_problems)

    for problem in problems:
        print(problem)

    if problems:
        print(f"\ncheck-release: {len(problems)} problem(s). A release is permanent.")
        return 1

    scope = ", ".join(["source"] + ["archives"] * args.dist + ["docs"] * args.docs)
    print(f"check-release: {version} is consistent ({scope} checked).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
