"""Benchmark settings: django-ox backend on PostgreSQL 16 (container ox-bench)."""

SECRET_KEY = "bench-only"
USE_TZ = True
DEBUG = False
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

INSTALLED_APPS = [
    "django_ox",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "bench_ox",
        "USER": "postgres",
        "PASSWORD": "ox",
        "HOST": "127.0.0.1",
        "PORT": "54330",
    }
}

# Backend at defaults: no QUEUES (any queue name allowed), no OPTIONS
# (MAX_ATTEMPTS=3, LOCK_TIMEOUT=300, BACKOFF_INITIAL=5, BACKOFF_MAX=600).
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
    }
}

# Capped at WARNING on the handler as well as the loggers, so a worker that
# raises a logger's level at its default verbosity still writes no per-task
# INFO lines. "django.tasks" is the framework's own logger (its signal
# receivers log one line per task start and finish). Same shape as
# settings_tasksdb.py.
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler", "level": "WARNING"}},
    "loggers": {
        "django.tasks": {"handlers": ["console"], "level": "WARNING"},
        "django_ox": {"handlers": ["console"], "level": "WARNING"},
    },
}
