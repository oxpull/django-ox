"""
Worker-death settings for django-tasks-db: settings_tasksdb.py with its own
database and nothing else changed.

The database is kill_tasksdb, not bench_tasksdb, for the reason given in
settings_kill_ox.py: the harnesses share the container, and a bench.py
cell must not be able to truncate a table under a kill trial in progress.
The backend itself stays at its defaults; there is no lease or timeout to
set.
"""

from benchsite.settings_tasksdb import *  # noqa: F403
from benchsite.settings_tasksdb import DATABASES as _BENCH_DATABASES

DATABASES = {"default": {**_BENCH_DATABASES["default"], "NAME": "kill_tasksdb"}}
