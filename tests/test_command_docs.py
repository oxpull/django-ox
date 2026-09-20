"""Every public command flag appears in its configuration table."""

import argparse
import re
from pathlib import Path

import pytest
from django.core.management import load_command_class

DOC = Path(__file__).resolve().parent.parent / "docs" / "configuration.md"

DJANGO_FLAGS = {
    "--force-color",
    "--help",
    "--no-color",
    "--pythonpath",
    "--settings",
    "--skip-checks",
    "--traceback",
    "--verbosity",
    "--version",
}


def public_flags(command_name: str) -> set[str]:
    command = load_command_class("django_ox", command_name)
    parser = command.create_parser("manage.py", command_name)
    return {
        option
        for action in parser._actions
        if action.help != argparse.SUPPRESS
        for option in action.option_strings
        if option.startswith("--") and option not in DJANGO_FLAGS
    }


#: The flag in a table row's first cell, read loosely on purpose. A row that
#: documents the flag with its value (`--format json`), pads its columns to
#: line them up, emphasises the cell or names a short alias beside the long
#: one still documents the flag. Matching one exact spelling would turn a
#: correct future table red over a row the reader can see sitting there.
#: `[^|]*?` cannot cross a pipe, so this stays inside the first cell.
FLAG_CELL = re.compile(r"^[ \t]*\|[^|]*?`(--[\w-]+)", re.MULTILINE)


def documented_flags(command_name: str) -> set[str]:
    match = re.search(
        rf"^## {re.escape(command_name)}[ \t]*$(?P<section>.*?)(?=^## |\Z)",
        DOC.read_text(),
        flags=re.MULTILINE | re.DOTALL,
    )
    assert match is not None, (
        f"no configuration section for {command_name} in {DOC.name}"
    )
    return set(FLAG_CELL.findall(match["section"]))


@pytest.mark.parametrize("command_name", ["ox_worker", "ox_prune", "ox_health"])
def test_every_public_command_flag_is_documented(command_name):
    parser_flags = public_flags(command_name)
    table_flags = documented_flags(command_name)
    assert len(parser_flags) >= 5, (
        f"{command_name} parser exposed only {len(parser_flags)} public flags"
    )
    assert len(table_flags) >= 5, (
        f"{command_name} configuration table contained only {len(table_flags)} flags"
    )
    missing = sorted(parser_flags - table_flags)
    assert not missing, (
        f"{command_name} flags missing from its {DOC.name} table: {missing}"
    )


# The comparison is deliberately one-way. A table may keep an explanatory or
# compatibility row that is not a current parser option; this contract only
# requires every public parser flag to remain documented.
