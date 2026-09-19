"""
Worker-death settings for django-ox: settings_ox.py with LOCK_TIMEOUT 15 s
and its own database.

Everything else (engine, host, DEBUG, USE_TZ, logging) is inherited from
settings_ox.py so the two harnesses cannot drift apart.

LOCK_TIMEOUT is set to 15 s in place of the 300 s default so a killed
worker's lease expires inside a trial's observation window. killbench.py
reads the same constant, records it in the results file, and derives the
reap interval from it the way worker.py does (min(30, max(LOCK_TIMEOUT / 2,
1))).

The database is kill_ox, not bench_ox: both harnesses share the container,
and a bench.py cell truncating bench_tasksdb under a kill trial in progress
is what made the split necessary.
"""

from benchsite.settings_ox import *  # noqa: F403
from benchsite.settings_ox import DATABASES as _BENCH_DATABASES

DATABASES = {"default": {**_BENCH_DATABASES["default"], "NAME": "kill_ox"}}

TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "OPTIONS": {
            "LOCK_TIMEOUT": 15.0,
        },
    }
}
