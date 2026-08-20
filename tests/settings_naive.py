from .settings import *  # noqa: F403

# Verification settings for USE_TZ=False, where the database's clock and the
# columns can disagree. SQLite's Now() is always UTC while every other writer
# fills these columns with naive local time, so a row that finished a second
# ago can read hours old. Nothing in the default suite sees that, because
# tests/settings.py leaves USE_TZ on.
#
# Run it with the process clock away from UTC, or the two agree by accident and
# the whole class of bug is invisible:
#
#   TZ=Pacific/Kiritimati DJANGO_SETTINGS_MODULE=tests.settings_naive \
#       .venv/bin/python -m pytest
#
# TIME_ZONE has to name the same zone as TZ above. Under USE_TZ=False
# timezone.now() is datetime.now(), which reads the process timezone, while the
# database session timezone comes from TIME_ZONE; two different zones there is
# an incoherent configuration rather than a test of one.
USE_TZ = False
TIME_ZONE = "Pacific/Kiritimati"
