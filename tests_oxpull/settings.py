import os
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent

SECRET_KEY = "test-only"
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

INSTALLED_APPS = [
    "django_ox",
    "oxpull",
]

# File-based SQLite, mirroring django-ox's test settings: threads in the
# concurrency tests need their own connections, and Django's shared-cache
# in-memory test database raises "database table is locked" across
# threads regardless of the busy timeout.
DATABASES: dict[str, dict[str, Any]] = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
        "OPTIONS": {"timeout": 20},
        "TEST": {"NAME": BASE_DIR / "test.sqlite3"},
    },
    # A deliberately separate, never-written database. Tests activate a
    # read router pointing at it to prove the dedup path pins its reads
    # to the write database (a lagged replica must not fake staleness).
    "replica": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "replica.sqlite3",
        "OPTIONS": {"timeout": 20},
        "TEST": {"NAME": BASE_DIR / "test_replica.sqlite3"},
    },
}

# Two runs of one leg against the same server must not share a test
# database: the second drops the first one's out from under it, or fails
# to create one that is already in use. Given OX_TEST_DB_NAME, the default
# alias tests against exactly that name and the replica against that name
# with "_replica" appended, so concurrent runs stay apart and the replica
# stays the separate, never-written database the routing tests need. The
# default alias takes the name as NAME as well, which is how django-ox
# hands a test database to a process that does not run the test runner.
# With the variable unset, which is how CI runs, every name is the fixed
# one it has always been.
if "OX_TEST_DB_NAME" in os.environ:
    DATABASES["default"]["NAME"] = os.environ["OX_TEST_DB_NAME"]
    DATABASES["default"]["TEST"]["NAME"] = os.environ["OX_TEST_DB_NAME"]
    DATABASES["replica"]["TEST"]["NAME"] = os.environ["OX_TEST_DB_NAME"] + "_replica"

TASKS = {
    "default": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["default", "emails"],
        "OPTIONS": {
            "MAX_ATTEMPTS": 3,
            "UNIQUE_TASKS": ["tests_oxpull.tasks.configured_unique"],
            # Batches and workflows refuse to be created or sealed unless
            # some django-ox backend schedules their reconciler, as a
            # project must. The test workers call run_once(), which never
            # dispatches a schedule, so these tick nothing here.
            "SCHEDULES": {
                "oxpull-batch-reconcile": {
                    "task": "oxpull.batches.reconcile",
                    "cron": "* * * * *",
                },
                "oxpull-workflow-reconcile": {
                    "task": "oxpull.workflows.reconcile",
                    "cron": "* * * * *",
                },
            },
        },
    },
    # A non-django-ox backend, so the batch guard against members that
    # never get an OxTask row has something real to refuse.
    "immediate": {"BACKEND": "django.tasks.backends.immediate.ImmediateBackend"},
    # Workflows are declared per backend, and the default backend stays
    # free of them on purpose: the rate-limit tests pin that a backend
    # configuring neither feature emits exactly the base worker's claim SQL,
    # and a gate on "default" would quietly break that promise.
    "flows": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["default", "emails"],
        "OPTIONS": {
            "MAX_ATTEMPTS": 3,
            "WORKER_CLASS": "oxpull.worker.OxpullWorker",
            "WORKFLOWS": True,
        },
    },
    # Workflows with the blocked-node deferral turned on. A separate
    # alias on purpose: "flows" stays the default-shaped backend, so the
    # matched gated/ungated pair that proves the claim gate is
    # load-bearing keeps measuring the gate and not the deferral.
    "flows_deferred": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["default", "emails"],
        "OPTIONS": {
            "MAX_ATTEMPTS": 3,
            "WORKER_CLASS": "oxpull.worker.OxpullWorker",
            "WORKFLOWS": True,
            "DEFER_BLOCKED_NODES": True,
        },
    },
    # Declares workflows but names no gating worker: oxpull.E007.
    "flows_ungated": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["default"],
        "OPTIONS": {"WORKFLOWS": True},
    },
    # Only a worker for the backend that configures a limit applies it,
    # though its window is shared by name. This one carries a limit so
    # that "default" can stay genuinely limit-free, which is what the
    # zero-cost test measures against.
    "limited": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["default"],
        "OPTIONS": {
            "WORKER_CLASS": "oxpull.worker.OxpullWorker",
            "RATE_LIMITS": {
                "hourly": {
                    "limit": 1,
                    "per": 3600,
                    "tasks": [
                        "tests_oxpull.tasks.limited_echo",
                        "tests_oxpull.tasks.limited_add",
                    ],
                },
            },
        },
    },
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "loggers": {
        "django_ox": {"handlers": ["console"], "level": "WARNING"},
    },
}
