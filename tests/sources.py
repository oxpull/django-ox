"""Schedule sources a test can name in OPTIONS["SCHEDULE_SOURCE"]."""

import builtins
import os

from django_ox.schedules import SettingsScheduleSource
from django_ox.stored import DatabaseScheduleSource


class FailingReadSource(SettingsScheduleSource):
    """
    The settings schedules, read by a dispatch pass through a read that
    raises: a shared read of the pass that nothing guards, as a reader
    added later might be. For worker processes, which take their
    configuration from the environment.

    OX_TEST_SOURCE_RAISES names the exception, a builtin class, and
    OX_TEST_SOURCE_PASSES how many dispatch passes it fails, or "every".
    The read made when the worker is built goes through.
    """

    def __init__(self, options, backend_alias):
        super().__init__(options, backend_alias)
        self.reads = 0

    def schedules(self):
        self.reads += 1
        error = getattr(builtins, os.environ["OX_TEST_SOURCE_RAISES"])
        passes = os.environ["OX_TEST_SOURCE_PASSES"]
        if self.reads > 1 and (passes == "every" or self.reads <= 1 + int(passes)):
            raise error("a shared read of the dispatch pass failed")
        return super().schedules()


class UnavailableAfterStart(SettingsScheduleSource):
    """
    A project's own source whose store answers when the worker is built and
    never again: every read after the first raises ConnectionError, as a
    source backed by a service that has gone away would.
    """

    def __init__(self, options, backend_alias):
        super().__init__(options, backend_alias)
        self.reads = 0

    def schedules(self):
        self.reads += 1
        if self.reads > 1:
            raise ConnectionError("the source's own store stopped answering")
        return super().schedules()


class RowSource(DatabaseScheduleSource):
    """A project's own source, under a name that is not the base class's."""


class CountingSource(DatabaseScheduleSource):
    """Records that this class, and not its base, built the schedule."""

    built = 0

    def _to_schedule(self, row):
        type(self).built += 1
        return super()._to_schedule(row)


class NotASource:
    """Importable, and not a schedule source."""


class DuckSource:
    """
    A source built by composition rather than inheritance.

    The worker's loader accepts any class it can build that answers
    schedules(), so this is a supported configuration and its rows are
    dispatched. It is not a DatabaseScheduleSource and has no
    _to_schedule.
    """

    def __init__(self, options, backend_alias):
        self._inner = DatabaseScheduleSource(options, backend_alias)

    def schedules(self):
        return self._inner.schedules()


class NoSchedulesMethod:
    """Builds from the same two arguments and answers nothing."""

    def __init__(self, options, backend_alias):
        self.options = options
