"""
The six shards of the oxpull feature suite (tests_oxpull/).

    python tools/ci/shard.py check          every test file is in exactly one shard
    python tools/ci/shard.py run shard3 SETTINGS [--hazard true|false]
                                            run one shard under pytest
    python tools/ci/shard.py files shard3 [all|hazard|not-hazard]
                                            the files of one shard, one line

A file belongs to the hazard group when "hazard" is in its name, the rule
tests_oxpull/conftest.py marks tests by. A mode of "not-hazard" drops those
files, so a shard made of nothing but hazard files prints nothing and the job
skips pytest instead of failing on its exit status 5 (no tests collected).

Whole files stay together because a file's tests share fixtures and, in a few
files, state that one test leaves for the next. Membership is in shards.json,
fitted to measured durations. The check is what stops a new test file from
falling out of every shard: CI fails until the file is added to one.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SHARDS = ROOT / "tools" / "ci" / "shards.json"
TESTS = ROOT / "tests_oxpull"


def load() -> dict[str, list[str]]:
    shards: dict[str, list[str]] = json.loads(SHARDS.read_text())["shards"]
    return shards


def problems(shards: dict[str, list[str]]) -> list[str]:
    on_disk = {f"tests_oxpull/{p.name}" for p in TESTS.glob("test_*.py")}
    seen: dict[str, str] = {}
    found: list[str] = []
    for name, files in shards.items():
        for f in files:
            if f in seen:
                found.append(f"{f} is in {seen[f]} and {name}")
            seen[f] = name
    found += [
        f"{f} is in no shard: add it to tools/ci/shards.json"
        for f in sorted(on_disk - set(seen))
    ]
    found += [f"{f} is in a shard but not on disk" for f in sorted(set(seen) - on_disk)]
    return found


def select(shards: dict[str, list[str]], name: str, mode: str) -> list[str]:
    return [
        f
        for f in shards[name]
        if mode == "all" or ("hazard" in Path(f).name) == (mode == "hazard")
    ]


def run(shards: dict[str, list[str]], name: str, settings: str, *, hazard: bool) -> int:
    """
    Replace this process with pytest on one shard's files.

    exec, not a child process, so a SIGABRT from `timeout -s ABRT` reaches
    pytest and faulthandler dumps its threads. Without the hazard flag the
    hazard files are not passed at all, and `-m "not hazard"` stays on as the
    second guard; a shard of hazard files only then runs nothing and passes,
    where pytest alone would exit 5.
    """
    files = select(shards, name, "all" if hazard else "not-hazard")
    if not files:
        print(f"{name}: no files to run without the hazard suite")
        return 0
    argv = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"]
    argv += [f"--ds={settings}", *files]
    if not hazard:
        argv += ["-m", "not hazard"]
    os.chdir(ROOT)
    os.execv(sys.executable, argv)  # noqa: S606 - the point is to be pytest
    return 0  # pragma: no cover


def main(argv: list[str]) -> int:
    shards = load()
    if argv[:1] == ["check"]:
        found = problems(shards)
        if found:
            print("\n".join(found), file=sys.stderr)
            return 1
        print(f"{sum(map(len, shards.values()))} files in {len(shards)} shards")
        return 0
    if argv[:1] == ["files"] and 2 <= len(argv) <= 3 and argv[1] in shards:
        mode = argv[2] if len(argv) == 3 else "all"
        if mode not in ("all", "hazard", "not-hazard"):
            print(__doc__, file=sys.stderr)
            return 2
        files = select(shards, argv[1], mode)
        if files:
            print(*files)
        return 0
    if argv[:1] == ["run"] and len(argv) in (3, 5) and argv[1] in shards:
        hazard = len(argv) == 5 and argv[3] == "--hazard" and argv[4] == "true"
        if len(argv) == 5 and (
            argv[3] != "--hazard" or argv[4] not in ("true", "false")
        ):
            print(__doc__, file=sys.stderr)
            return 2
        return run(shards, argv[1], argv[2], hazard=hazard)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
