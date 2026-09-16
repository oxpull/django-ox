"""
The --database flag every django-ox command takes, and what it resolves to.

Two things have to hold for a command that works on the task table, and a
flag alone gives neither.

The alias is resolved once per invocation and every statement goes to it.
A command that leaves its reads unqualified follows ``db_for_read`` while
its writes follow ``db_for_write``. Under the read-replica router from
Django's own documentation those are two databases, so the command reads a
replica and writes the primary. A replica that is behind, which is what a
replica is, then makes ``ox_prune`` read rows it has already deleted and
``ox_health`` report a backlog that is not the one the workers see.

The system checks are scoped to that alias, the way ``migrate`` scopes
them. From Django 6.1 a command that names no alias runs the checks
against every alias in DATABASES, and checking a SQLite or MySQL alias
opens a connection. An alias the machine cannot reach ends the command
before it does any work, whatever ``--database`` said.
"""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import connections, router

from django_ox.models import OxTask


class DatabaseCommand(BaseCommand):
    """A command that works on one database alias."""

    database_help = (
        "Database alias to work on. Defaults to the alias the router sends "
        "OxTask writes to, which is 'default' unless you wrote a router."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--database", default=None, help=self.database_help)

    def default_database(self) -> str:
        """The alias to use when --database is not given."""
        return router.db_for_write(OxTask)

    def database(self, options: dict[str, Any]) -> str:
        alias: str | None = options.get("database")
        if alias is None:
            return self.default_database()
        if alias not in connections:
            known = ", ".join(sorted(connections))
            raise CommandError(
                f"No database alias {alias!r} in DATABASES. Known aliases: {known}."
            )
        return alias

    def get_check_kwargs(self, options: Any) -> dict[str | None, Any]:
        # Raised here rather than in handle(): the checks run first, and
        # connections[alias] on a name that is not there would end the
        # command with a traceback instead of a sentence.
        return {
            **super().get_check_kwargs(options),
            "databases": [self.database(options)],
        }
