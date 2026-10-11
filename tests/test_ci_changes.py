"""
The CI selection logic for the oxpull feature suite (tools/ci/changes.py) and
its shard list (tools/ci/shard.py).

Nothing here runs a database. A wrong answer from `classify` is a pull request
that skips tests it needed, which no later job can notice, so each rule has a
case and every "false" is asserted from a diff that should produce one.
"""

import ast
import json
from pathlib import Path

import pytest

from tools.ci import changes, shard


def flags(event, paths, ref=""):
    return changes.classify(event, ref, paths)


@pytest.mark.parametrize(
    ("paths", "feature", "hazard"),
    [
        (["src/django_ox/worker.py"], True, True),
        (["src/django_ox/models.py", "tests/test_x.py"], True, True),
        (["src/django_ox/migrations/0009_x.py"], True, True),
        (["src/oxpull/workflows.py"], True, True),
        (["src/django_ox/new_module.py"], True, True),  # unknown source runs more
        (["src/django_ox/admin.py"], True, True),
        (["src/django_ox/management/commands/ox_health.py"], True, True),
        (["src/django_ox/actions.py"], True, True),
        (["src/django_ox/views.py"], True, True),
        (["src/django_ox/urls.py"], True, True),
        (["src/django_ox/metrics.py"], True, True),
        (["src/django_ox/stats.py"], True, True),
        (["src/django_ox/durations.py"], True, True),
        (["src/django_ox/testing.py"], True, False),
        (
            ["src/django_ox/management/commands/ox_import_beat_schedules.py"],
            True,
            False,
        ),
        (["src/django_ox/management/_beat_cron.py"], True, False),
        (
            ["src/django_ox/testing.py", "src/django_ox/actions.py"],
            True,
            True,
        ),
        (["docs/index.md", "mkdocs.yml", "CHANGELOG.md"], False, False),
        (["tests/test_batches.py"], False, False),  # the base suite covers it
        (["README.md"], True, True),  # a hazard test reads the README
        ([".github/workflows/ci.yml"], True, True),
        (["tools/ci/shards.json"], True, True),
        (["pyproject.toml"], True, True),
        (["tests_oxpull/test_batches.py"], True, False),
        (["tests_oxpull/test_workflows_hazard_cancel_cas.py"], True, True),
        (["tests_oxpull/conftest.py"], True, True),
        (["tests_oxpull/fixtures/archive.json"], True, True),
        (["tools/some_new_script.sh"], True, False),  # unknown path: feature runs
        (["docs/a.md", "src/django_ox/worker.py"], True, True),  # one bad path decides
    ],
)
def test_a_pull_request_runs_what_its_paths_can_reach(paths, feature, hazard):
    result = flags("pull_request", paths)
    assert (result["feature"], result["hazard"], result["full"]) == (
        feature,
        hazard,
        False,
    )


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
COMMANDS = {
    path.stem: path.relative_to(ROOT).as_posix()
    for path in SRC.glob("*/management/commands/*.py")
    if not path.name.startswith("_")
}


def source_path(module):
    base = SRC.joinpath(*module.split("."))
    for path in (base.with_suffix(".py"), base / "__init__.py"):
        if path.is_file():
            return path.relative_to(ROOT).as_posix()
    return None


def imported_modules(tree, package):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name
        elif isinstance(node, ast.ImportFrom):
            module = node.module
            if node.level:
                parts = package.split(".")
                base = ".".join(parts[: len(parts) - node.level + 1])
                module = f"{base}.{module}" if module else base
            yield module
            for alias in node.names:
                yield f"{module}.{alias.name}"


def string_argument(call):
    if call.args and isinstance(call.args[0], ast.Constant):
        value = call.args[0].value
        return value if isinstance(value, str) else None
    return None


def references(path):
    tree = ast.parse(path.read_text(), filename=str(path))
    found = [
        (name, source_path(name)) for name in imported_modules(tree, "tests_oxpull")
    ]
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = string_argument(node)
        if name is None:
            continue
        func = node.func
        called = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if called == "call_command" and name in COMMANDS:
            found.append((f"call_command({name!r})", COMMANDS[name]))
        elif called == "include" and (target := source_path(name)):
            found.append((f"include({name!r})", target))
            package = (
                name if target.endswith("/__init__.py") else name.rpartition(".")[0]
            )
            urls = ast.parse((ROOT / target).read_text(), filename=target)
            found += [
                (f"include({name!r}) imports {module}", source_path(module))
                for module in imported_modules(urls, package)
            ]
    return [(reference, source) for reference, source in found if source]


def test_no_hazard_test_reaches_a_module_that_skips_the_hazard_jobs():
    hazard_files = sorted((ROOT / "tests_oxpull").glob("*hazard*.py"))
    assert hazard_files, "no tests_oxpull/*hazard*.py file found"
    reached = [
        (path.relative_to(ROOT).as_posix(), reference, source)
        for path in hazard_files
        for reference, source in references(path)
    ]
    assert reached, "the scan resolved no reference to a file under src/"
    excluded = [
        f"{hazard_file}: {reference} reaches {source}, excluded by SRC_NOT_HAZARD"
        for hazard_file, reference, source in reached
        if changes.SRC_NOT_HAZARD.match(source)
    ]
    assert not excluded, "\n".join(excluded)


def test_a_diff_that_cannot_be_computed_runs_everything_but_the_full_matrix():
    assert flags("pull_request", None) == {
        "feature": True,
        "hazard": True,
        "full": False,
    }


@pytest.mark.parametrize(
    ("event", "ref", "full"),
    [
        ("schedule", "refs/heads/main", True),
        ("workflow_dispatch", "refs/heads/main", True),
        ("push", "refs/heads/main", False),
        ("push", "refs/tags/rc1", True),
        ("push", "refs/tags/v1.9.0", True),
        ("something_new", "", False),
    ],
)
def test_other_events_run_the_hazard_suite_and_a_candidate_runs_the_matrix(
    event, ref, full
):
    result = flags(event, None, ref)
    assert result["feature"] and result["hazard"]
    assert result["full"] is full


def test_the_full_matrix_adds_the_grid_and_the_pooled_server():
    plain = changes.render(flags("pull_request", ["src/django_ox/worker.py"]))
    full = changes.render(flags("schedule", None))
    assert len(json.loads(plain["sqlite_cells"])) == 2
    assert len(json.loads(plain["postgres_cells"])) == 1
    assert len(json.loads(full["sqlite_cells"])) == 6
    labels = [cell["label"] for cell in json.loads(full["postgres_cells"])]
    assert labels.count("postgres") == 6
    assert labels.count("postgres pooled") == 1


def test_every_feature_test_file_is_in_exactly_one_shard():
    assert shard.problems(shard.load()) == []


def test_a_file_missing_from_every_shard_is_named():
    shards = shard.load()
    victim = shards["shard2"].pop()
    assert any(victim in line and "no shard" in line for line in shard.problems(shards))


def test_a_file_in_two_shards_is_named():
    shards = shard.load()
    shards["shard1"].append(shards["shard2"][0])
    victim = shards["shard2"][0]
    assert f"{victim} is in shard1 and shard2" in shard.problems(shards)
