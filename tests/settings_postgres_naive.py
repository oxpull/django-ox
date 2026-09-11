from .settings_postgres import *  # noqa: F403

# PostgreSQL with USE_TZ off, which is the one configuration where the claim
# and the renewal can end up on two different clocks.
#
# tests/settings_naive.py covers the same setting on SQLite and cannot reach
# this: it inherits tests/settings.py's DATABASES. The PostgreSQL claim is a
# single hand-written statement that stamps locked_at from the server, while
# renew_leases and the reaper cutoff take _lease_now(), which is the worker's
# own clock when USE_TZ is off. Nothing in the default suite compares the two,
# because tests/settings.py leaves USE_TZ on and both clocks are then the
# database's.
#
# Run it with the process clock away from the server's, or the two agree by
# accident and the whole class of defect is invisible:
#
#   TZ=Pacific/Kiritimati DJANGO_SETTINGS_MODULE=tests.settings_postgres_naive
#       .venv/bin/python -m pytest -q tests/test_lease_clock.py
#
# TIME_ZONE names the same zone as TZ for the reason settings_naive.py gives:
# under USE_TZ=False timezone.now() reads the process timezone while the
# database session timezone comes from TIME_ZONE, and two different zones
# there is an incoherent deployment rather than a test of one.
USE_TZ = False
TIME_ZONE = "Pacific/Kiritimati"
