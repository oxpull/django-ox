"""
The upgrade path, from the schema a released version left behind.

Every other test in this suite runs against a database Django built by
applying all migrations to an empty file. That is not the path anyone
takes. A user arrives with the schema their last release created and
runs `migrate`, and nothing here exercised that until this file.

0.3.1 is the last published version and it shipped 0001 and 0002 only.
Migration 0003 creates the table rate limiting counts in; it went out in
0.4.0, which was never published. An installation with the earlier oxpull
0.3.1 schema has an unapplied 0003, and the CHANGELOG tells them to run `migrate oxpull`
before enabling RATE_LIMITS. This asserts what happens if they do, and
what happens if they do not.
"""

import pytest
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.db.utils import DatabaseError, OperationalError, ProgrammingError
from django.utils import timezone

from oxpull.models import OxpullRateWindow

# The state a 0.3.1 install is in: batches exist, rate limiting does not.
PUBLISHED_0_3_1 = "0002_oxpullbatch_oxpullbatchmember"

MISSING_TABLE_ERRORS = (OperationalError, ProgrammingError, DatabaseError)


def _latest_migration() -> str:
    """
    The newest oxpull migration, read from the graph rather than named.

    Restoring to a hard-coded migration silently rolls back every one
    added after it was written, which breaks the tests that run next
    rather than this file. Asking the graph cannot go stale.
    """
    executor = MigrationExecutor(connection)
    executor.loader.build_graph()
    leaves = [
        name for app, name in executor.loader.graph.leaf_nodes() if app == "oxpull"
    ]
    assert len(leaves) == 1, f"expected one oxpull leaf migration, got {leaves}"
    return leaves[0]


def _migrate(target: str) -> None:
    """Move the oxpull app to `target`."""
    executor = MigrationExecutor(connection)
    executor.loader.build_graph()
    executor.migrate([("oxpull", target)])


@pytest.fixture
def at_published_schema():
    """
    Rewinds oxpull to the 0.3.1 schema for one test, then restores it.

    The restore runs even when the test fails, because a database left at
    0002 breaks every test that follows it in the same session.
    """
    latest = _latest_migration()
    _migrate(PUBLISHED_0_3_1)
    try:
        yield
    finally:
        _migrate(latest)


@pytest.mark.django_db(transaction=True)
def test_the_published_schema_has_no_rate_limit_table(at_published_schema):
    """
    Pins the premise. If 0003 ever moves into an earlier migration this
    fails, and the rest of this file stops meaning anything.
    """
    assert OxpullRateWindow._meta.db_table not in connection.introspection.table_names()


@pytest.mark.django_db(transaction=True)
def test_using_rate_limits_before_migrating_fails_on_the_missing_table(
    at_published_schema,
):
    """
    The footgun the CHANGELOG warns about, made concrete: on the 0.3.1
    schema the limiter's own table is not there, and the failure is a
    database error rather than anything oxpull can explain.
    """
    with pytest.raises(MISSING_TABLE_ERRORS):
        OxpullRateWindow.objects.filter(limit_name="anything").exists()


@pytest.mark.django_db(transaction=True)
def test_migrating_from_the_published_schema_creates_the_rate_limit_table():
    """
    The upgrade from the earlier oxpull 0.3.1 schema, then
    `migrate`. The table has to exist afterwards and be usable, not merely
    be recorded as applied.
    """
    _migrate(PUBLISHED_0_3_1)
    try:
        assert (
            OxpullRateWindow._meta.db_table
            not in connection.introspection.table_names()
        )
        _migrate(_latest_migration())
        assert OxpullRateWindow._meta.db_table in connection.introspection.table_names()

        # Applied is not the same as working: write and read a window.
        OxpullRateWindow.objects.create(
            limit_name="upgrade-check",
            window_started_at=timezone.now(),
            used=1,
        )
        assert OxpullRateWindow.objects.get(limit_name="upgrade-check").used == 1
        OxpullRateWindow.objects.filter(limit_name="upgrade-check").delete()
    finally:
        _migrate(_latest_migration())


@pytest.mark.django_db(transaction=True)
def test_the_upgrade_leaves_batch_rows_alone():
    """
    0003 adds a table and must not touch the batch table containing the existing
    `oxpull` 0.3.1 data. A row written before the upgrade survives it.
    """
    from oxpull.models import OxpullBatch

    table = OxpullRateWindow._meta.db_table
    _migrate(PUBLISHED_0_3_1)
    try:
        # The row is written on the old schema, which is the whole point:
        # asserting it survives is only meaningful if a schema change
        # actually happened around it.
        assert table not in connection.introspection.table_names()
        batch = OxpullBatch.objects.create(
            name="pre-upgrade", created_at=timezone.now()
        )

        _migrate(_latest_migration())

        assert table in connection.introspection.table_names()
        assert OxpullBatch.objects.filter(pk=batch.pk).exists()
        assert OxpullBatch.objects.get(pk=batch.pk).name == "pre-upgrade"
    finally:
        _migrate(_latest_migration())
