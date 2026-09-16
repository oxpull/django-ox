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

That second part holds for the commands that probe and report. It must
not hold for ``ox_worker``, which is a daemon: see
``checks_the_database`` below.
"""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import DatabaseError, connections, router

from django_ox.models import OxTask


class DatabaseCommand(BaseCommand):
    """A command that works on one database alias."""

    #: Do the system checks run against the alias this command works on?
    #:
    #: A command that probes says yes. ``ox_prune`` and ``ox_health`` run
    #: once and report, so a database they cannot reach is the answer
    #: rather than an obstacle, and a check that opens the connection
    #: reaches it first.
    #:
    #: ``ox_worker`` says no, because it is a daemon. It already handles a
    #: database that goes away: the pass logs ``worker_poll_failed``, the
    #: connection is dropped and the next poll reconnects, so a worker
    #: rides out a restart and picks the queue up again. A check that
    #: opened the connection at startup would end the process instead, and
    #: a rolling restart of the database would take every worker with it.
    checks_the_database = True

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

    def check(self, *args: Any, **kwargs: Any) -> None:
        """
        Run the system checks, and say so in a line if the database is down.

        Scoping the checks to an alias is what opens the connection, so a
        database that is not there is reported from inside the check
        framework, as a driver traceback. These commands run from cron
        lines and container probes, where a traceback is the least
        readable thing that could arrive; every other failure they have is
        one line, and so is this one.
        """
        try:
            super().check(*args, **kwargs)
        except DatabaseError as exc:
            raise CommandError(f"Database unreachable: {exc}") from exc

    def get_check_kwargs(self, options: Any) -> dict[str | None, Any]:
        if not self.checks_the_database:
            # An empty list rather than no key at all. From Django 6.1 a
            # command that names nothing has the checks that take a
            # database run against every alias in DATABASES, and one of
            # those is enough to end the process before the first poll.
            return {**super().get_check_kwargs(options), "databases": []}
        # Raised here rather than in handle(): the checks run first, and
        # connections[alias] on a name that is not there would end the
        # command with a traceback instead of a sentence.
        return {
            **super().get_check_kwargs(options),
            "databases": [self.database(options)],
        }
