"""
Decide which parts of the oxpull feature suite (tests_oxpull/) a CI run needs.

Run by the `changes` job in .github/workflows/ci.yml:

    python tools/ci/changes.py            # reads the environment, writes $GITHUB_OUTPUT
    python tools/ci/changes.py --files F  # dry run: the changed paths come from file F
                                          # ("-" for stdin), the event from --event

Outputs, one `name=value` line each:

    feature  "true" unless every changed path is known to be inert for the suite
    hazard   "true" when the hazard tests (marker `hazard`) must run too
    full     "true" when the hazard suite must run on the whole matrix
    sqlite_cells, postgres_cells   the matrix cells, as JSON

Every rule fails towards running more. A path no rule knows runs the feature
suite. A diff that cannot be computed turns everything on. The only way to a
`false` is a changed-path list in which every path matched an inert pattern
(feature) or matched no source path at all (hazard).

Why the hazard tests are not simply always on: they are about 78 percent of
the feature suite's time (PostgreSQL: 2,219 of 2,855 measured seconds), and
most changes cannot reach what they pin. They run on main, nightly, on a
release candidate and when a change touches the code they pin.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

# Paths that cannot change what tests_oxpull does. README.md is NOT here: a
# hazard test reads it. tests/ is not here for the opposite reason in a
# different column: tests_oxpull imports nothing from it, so a change that only
# touches the base suite does not need the feature suite (see FREE_ONLY).
INERT = re.compile(
    r"""^(
        docs/ | overrides/ | benchmarks/ | brand/
      | mkdocs\.yml | context7\.json | LICENSE | CHANGELOG\.md | CODE_OF_CONDUCT\.md
      | CONTRIBUTING\.md | SECURITY\.md | AGENTS\.md
      | \.github/ISSUE_TEMPLATE/ | \.github/PULL_REQUEST_TEMPLATE\.md
      | \.github/FUNDING\.yml | \.github/dependabot\.yml
      | tests/
    )""",
    re.VERBOSE,
)

# Skip change-selected hazard jobs only for the modules below, which are not
# reached by the hazard suite's static reference scan. Imports, command calls,
# and URL includes count; mounting django_ox.urls also reaches its views.
# tests/test_ci_changes.py guards this list against new hazard references.
# This is a static check, not proof of runtime isolation. Nightly, rc-tag,
# and manual-dispatch runs still execute every hazard test.
SRC_NOT_HAZARD = re.compile(
    r"""^src/django_ox/(
        testing\.py
      | management/_beat_.* | management/commands/ox_import_beat_schedules\.py
    )$""",
    re.VERBOSE,
)

# Paths that change what the hazard tests run against or how.
HAZARD_PATHS = re.compile(
    r"""^(
        src/(django_ox|oxpull)/.* | pyproject\.toml | \.github/workflows/
      | tools/ci/ | README\.md
      | tests_oxpull/(?!test_)   # conftest, settings, helpers, fixtures
      | tests_oxpull/test_[^/]*hazard
    )""",
    re.VERBOSE,
)

CORNERS = [
    {"python": "3.12", "django": "6.0"},
    {"python": "3.14", "django": "6.1"},
]
FULL_GRID = [
    {"python": py, "django": dj}
    for py in ("3.12", "3.13", "3.14")
    for dj in ("6.0", "6.1")
]


def classify(event: str, ref: str, paths: list[str] | None) -> dict[str, bool]:
    """
    `paths` is the changed-path list of a pull request, or None when it could
    not be computed (or, for any other event, is not meaningful).
    """
    if event in ("schedule", "workflow_dispatch"):
        return {"feature": True, "hazard": True, "full": True}
    if event == "push":
        released = ref.startswith(("refs/tags/rc", "refs/tags/v"))
        return {"feature": True, "hazard": True, "full": released}
    # pull_request, or anything unknown.
    if event != "pull_request" or paths is None:
        return {"feature": True, "hazard": True, "full": False}
    feature = any(not INERT.match(p) for p in paths)
    hazard = False
    for p in paths:
        if p.startswith("src/") and SRC_NOT_HAZARD.match(p):
            continue
        if HAZARD_PATHS.match(p):
            hazard = True
    # A workflow or script change reaches everything below it.
    return {"feature": feature or hazard, "hazard": hazard, "full": False}


def cells(*, full: bool) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    plain = "tests_oxpull.settings_postgres"
    pooled = "tests_oxpull.settings_postgres_pooled"
    sqlite = FULL_GRID if full else CORNERS
    newest = {"python": "3.14", "django": "6.1"}
    postgres = [{**newest, "settings": plain, "label": "postgres"}]
    if full:
        postgres = [
            {**cell, "settings": plain, "label": "postgres"} for cell in FULL_GRID
        ] + [{**newest, "settings": pooled, "label": "postgres pooled"}]
    return sqlite, postgres


def changed_paths(base: str) -> list[str] | None:
    try:
        out = subprocess.run(  # noqa: S603
            ["git", "diff", "--name-only", f"{base}...HEAD"],  # noqa: S607
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (subprocess.CalledProcessError, OSError):
        return None
    return [line for line in out.splitlines() if line]


def render(flags: dict[str, bool]) -> dict[str, str]:
    sqlite, postgres = cells(full=flags["full"])
    return {
        "feature": str(flags["feature"]).lower(),
        "hazard": str(flags["hazard"]).lower(),
        "full": str(flags["full"]).lower(),
        "sqlite_cells": json.dumps(sqlite, separators=(",", ":")),
        "postgres_cells": json.dumps(postgres, separators=(",", ":")),
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--event", default=os.environ.get("EVENT", ""))
    parser.add_argument("--ref", default=os.environ.get("REF", ""))
    parser.add_argument("--base", default=os.environ.get("BASE_SHA", ""))
    parser.add_argument(
        "--files", help="dry run: read changed paths from this file, - for stdin"
    )
    args = parser.parse_args(argv)

    paths: list[str] | None
    if args.files:
        text = sys.stdin.read() if args.files == "-" else Path(args.files).read_text()
        paths = [line.strip() for line in text.splitlines() if line.strip()]
    elif args.event == "pull_request" and args.base:
        paths = changed_paths(args.base)
    else:
        paths = None

    outputs = render(classify(args.event, args.ref, paths))
    if paths is not None:
        print("changed paths:", *paths, sep="\n  ", file=sys.stderr)
    lines = [f"{key}={value}" for key, value in outputs.items()]
    target = os.environ.get("GITHUB_OUTPUT")
    if target and not args.files:
        with Path(target).open("a") as handle:
            handle.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
