"""Maintenance tasks for django-ox."""

from __future__ import annotations

import io
import json
from typing import Any, cast

from django.core.management import call_command

from django_ox.compat import task


@task
def prune(
    older_than: str = "7d",
    include_failed: bool = False,  # noqa: FBT001, FBT002
    queue: str | None = None,
    dry_run: bool = False,  # noqa: FBT001, FBT002
    batch_size: int | None = None,
) -> dict[str, Any]:
    """
    Prune finished task rows and old dispatch history.

    Runs ``ox_prune`` with ``--format json`` and returns the result dictionary.
    """
    out = io.StringIO()
    args: list[str] = [
        "--format",
        "json",
        "--older-than",
        str(older_than),
    ]
    if include_failed:
        args.append("--include-failed")
    if queue is not None:
        args.extend(["--queue", str(queue)])
    if dry_run:
        args.append("--dry-run")
    if batch_size is not None:
        args.extend(["--batch-size", str(batch_size)])

    # The unreadable-tick warning goes to stderr; the result carries its counts.
    call_command("ox_prune", *args, stdout=out, stderr=io.StringIO())
    return cast(dict[str, Any], json.loads(out.getvalue()))
