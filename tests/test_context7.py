"""context7.json must satisfy the limits Context7 publishes for it.

Context7 serves this package's docs and rules to coding assistants. A file
that violates its schema is rejected whole, and the previous version keeps
being served; no error is reported anywhere, and the refresh endpoint answers
200 either way. So the schema is the gate, and it has to be checked here.

The limits below are transcribed from https://context7.com/schema/context7.json
(draft-07), read 2026-09-09. They are hard-coded rather than fetched so the
check does not need the network and cannot pass because a request failed.
"""

import json
from pathlib import Path

CONFIG = Path(__file__).resolve().parent.parent / "context7.json"

DESCRIPTION_MAX = 200
PROJECT_TITLE_MAX = 100
RULE_MAX = 255
RULES_MAX_ITEMS = 50
FOLDER_MAX = 255
FOLDERS_MAX_ITEMS = 50


def config():
    return json.loads(CONFIG.read_text())


def test_is_valid_json():
    assert isinstance(config(), dict)


def test_description_within_limit():
    value = config()["description"]
    assert 10 <= len(value) <= DESCRIPTION_MAX, f"{len(value)} characters"


def test_project_title_within_limit():
    value = config()["projectTitle"]
    assert 1 <= len(value) <= PROJECT_TITLE_MAX, f"{len(value)} characters"


def test_every_rule_within_limit():
    over = {i: len(r) for i, r in enumerate(config()["rules"], 1) if len(r) > RULE_MAX}
    assert not over, f"rules over {RULE_MAX} characters, by 1-based index: {over}"


def test_rules_count_within_limit():
    rules = config()["rules"]
    assert rules, "no rules declared"
    assert len(rules) <= RULES_MAX_ITEMS, f"{len(rules)} rules"


def test_folder_lists_within_limits():
    data = config()
    for key in ("folders", "excludeFolders"):
        values = data.get(key, [])
        assert len(values) <= FOLDERS_MAX_ITEMS, f"{key}: {len(values)} entries"
        for value in values:
            assert 1 <= len(value) <= FOLDER_MAX, f"{key}: {value!r}"


def test_exclude_files_are_bare_filenames():
    # The schema constrains each entry with ^[^/\\]+$: a name, never a path.
    for value in config().get("excludeFiles", []):
        assert "/" not in value and "\\" not in value, value
        assert 1 <= len(value) <= FOLDER_MAX, f"{value!r}"


def test_rules_do_not_contradict_the_supported_django_floor():
    # The package requires Django 5.2 or later. A rule naming a higher floor
    # is the most damaging thing this file can tell an assistant, because it
    # reads as a reason not to install.
    joined = " ".join(config()["rules"])
    assert "Django 5.2" in joined, "no rule states the 5.2 floor"
    assert "Requires Django 6.0" not in joined
