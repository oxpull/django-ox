"""The lease clock and the columns it writes into must agree under any USE_TZ.

Lease timestamps are stamped database-side so that a lease written on one host
is judged against the same clock on another. That only holds while the
database's clock matches what the columns already contain. Django's Now()
compiles to STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') on SQLite, and SQLite's 'now'
is always UTC, so under USE_TZ=False it writes UTC into columns every other
writer fills with naive local time. Everything that measures an age against
timezone.now() is then wrong by the machine's whole UTC offset.

These assert what an operator would actually see rather than the offset itself:
a prune that deletes live rows, and a health check that can no longer fail. The
default settings leave USE_TZ on, where the two clocks agree and these prove
little, so the subclass at the bottom turns it off and is where they bite.
"""

import os
import time
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.stats import last_claim_age

from .tasks import add

# A row here is stamped and measured within milliseconds, so seconds of drift
# already means the two clocks disagree; scheduling noise does not reach that
# far. What this guards against is a whole UTC offset, an hour at the least.
TOLERANCE = timedelta(seconds=30)


def run_one(worker):
    add.enqueue(1, 2)
    assert worker.run_once() is True
    row = OxTask.objects.get()
    assert row.status == OxTask.Status.SUCCESSFUL
    return row


@pytest.mark.django_db
class TestLeaseClockAgreesWithColumns:
    def test_finished_at_agrees_with_the_process_clock(self, worker):
        row = run_one(worker)
        drift = abs(timezone.now() - row.finished_at)
        assert drift < TOLERANCE, (
            f"finished_at sits {drift} from the process clock, so every age "
            "measured against timezone.now() is wrong by that much."
        )

    def test_a_row_that_just_finished_is_not_an_hour_old(self, worker):
        run_one(worker)
        call_command("ox_prune", "--older-than", "1h", stdout=StringIO())
        assert OxTask.objects.count() == 1, (
            "ox_prune --older-than 1h deleted a row that finished this "
            "instant. Under a clock disagreement the row reads a whole UTC "
            "offset old, and a delete is not reversible."
        )

    def test_last_claim_age_is_small_in_either_direction(self, worker):
        # The two sides of this reading come from two clocks: timezone.now()
        # is this process, last_attempted_at was stamped by the database. The
        # gap between the claim and the reading is a few milliseconds, so a
        # server whose clock is milliseconds ahead -- one on another host, or
        # one in a VM that drifts from the host it runs on -- reads as a
        # negative age for reasons that are nobody's bug. The bound is the
        # same in both directions for that reason. What it is here to catch
        # is a whole UTC offset, an hour at the least, and TOLERANCE is
        # nowhere near an hour either way.
        run_one(worker)
        age = last_claim_age()
        assert age is not None
        assert -TOLERANCE < age < TOLERANCE, (
            f"last_claim_age reports {age}, so ox_health --worker-timeout "
            "either always fails or can never fail."
        )


@pytest.fixture
def naive_local_time(settings, monkeypatch):
    """USE_TZ off, with the clock a long way from UTC.

    Both the process timezone and TIME_ZONE move, and they have to move
    together. Under USE_TZ=False timezone.now() is datetime.now(), which reads
    the process timezone and ignores the Django setting, while the database
    session timezone is set from TIME_ZONE. Naming two different zones there
    is incoherent before any django-ox code runs, so a test that moved only
    one would be reporting its own misconfiguration.

    Neither is UTC on purpose: at UTC every clock in play agrees by accident,
    so a test that only flipped USE_TZ would pass everywhere and prove
    nothing. UTC+14 makes any disagreement unmissable.
    """
    original = os.environ.get("TZ")
    os.environ["TZ"] = "Pacific/Kiritimati"
    time.tzset()
    settings.USE_TZ = False
    settings.TIME_ZONE = "Pacific/Kiritimati"
    try:
        yield
    finally:
        if original is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = original
        time.tzset()


@pytest.mark.skipif(
    not hasattr(time, "tzset"), reason="tzset is POSIX only; no way to force the clock"
)
class TestLeaseClockUnderNaiveDatetimes(TestLeaseClockAgreesWithColumns):
    """The same three assertions with USE_TZ off, which is where they bite.

    tests/settings_naive.py runs the whole suite this way. Inheriting them here
    keeps the guard alive in a plain pytest run too, so the case cannot go
    unwatched because nobody remembered the second settings module.
    """

    @pytest.fixture(autouse=True)
    def _use_naive_local_time(self, naive_local_time):
        pass
