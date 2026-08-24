import json
import os
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent

SECRET_KEY = "test-only"
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django_ox",
]

# The admin stack, for tests/test_admin.py. django_ox itself needs none of it.
MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]
ROOT_URLCONF = "tests.urls"
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "django.template.context_processors.request",
            ]
        },
    }
]

# File-based SQLite, including for the test database: worker threads need
# their own connections, and Django's default shared-cache in-memory test
# database raises "database table is locked" across threads regardless of
# the busy timeout.
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
        "OPTIONS": {"timeout": 20},
        # Named per session: two pytest runs on one checkout must not share
        # a test database. Worker subprocesses do not recompute this; they
        # get the parent's name through OX_TEST_DB_NAME below.
        "TEST": {"NAME": BASE_DIR / f"test_{os.getpid()}.sqlite3"},
    }
}

TASKS: dict[str, dict[str, Any]] = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "QUEUES": ["default", "emails"],
        "OPTIONS": {"MAX_ATTEMPTS": 3},
    }
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "loggers": {
        # Worker processes started from the test suite raise this to INFO to
        # assert on lifecycle lines.
        "django_ox": {
            "handlers": ["console"],
            "level": os.environ.get("OX_TEST_LOG_LEVEL", "WARNING"),
        },
    },
}

# Worker processes started from the test suite must hit the test database the
# parent created, not the development one; the parent passes its name through.
if "OX_TEST_DB_NAME" in os.environ:
    DATABASES["default"]["NAME"] = os.environ["OX_TEST_DB_NAME"]

# Worker processes started from the test suite take extra backend OPTIONS
# (a task timeout, say) as a JSON object, since they only see settings.
if "OX_TEST_TASKS_OPTIONS" in os.environ:
    TASKS["default"]["OPTIONS"].update(json.loads(os.environ["OX_TEST_TASKS_OPTIONS"]))
