"""Build hooks for the source distribution.

Hatchling force-includes the repository's VCS ignore files in every sdist, and
a forced include is not subject to the target's `exclude` list. The sdist is
what PyPI serves, so it carries the package and nothing else the include list
does not name.
"""

from __future__ import annotations

from pathlib import PurePath
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

VCS_IGNORE_FILES = frozenset({".gitignore", ".hgignore"})


class SdistBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        forced = build_data["force_include"]
        for source in list(forced):
            if PurePath(source).name in VCS_IGNORE_FILES:
                del forced[source]
