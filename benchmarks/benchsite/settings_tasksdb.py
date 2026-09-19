"""Benchmark settings: django-tasks-db backend on PostgreSQL 16 (container ox-bench).

Configured per the django-tasks-db 0.13.0 README (its packaged METADATA):
INSTALLED_APPS gets "django_tasks_db", TASKS points at
"django_tasks_db.DatabaseBackend". Everything else is left at defaults.
"""

SECRET_KEY = "bench-only"
USE_TZ = True
DEBUG = False
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

INSTALLED_APPS = [
    "django_tasks_db",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "bench_tasksdb",
        "USER": "postgres",
        "PASSWORD": "ox",
        "HOST": "127.0.0.1",
        "PORT": "54330",
    }
}

TASKS = {
    "default": {
        "BACKEND": "django_tasks_db.DatabaseBackend",
    }
}

# Capped at WARNING on the handler as well as the loggers. db_worker sets
# both "django.tasks" (the core framework logger since 0.13.0; "django_tasks"
# was the backport's) and "django_tasks_db" to INFO at its default
# verbosity, which overrides a cap on the logger alone; the handler cap
# keeps the framework's per-task lines out of the worker logs. Same shape
# as settings_ox.py.
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler", "level": "WARNING"}},
    "loggers": {
        "django.tasks": {"handlers": ["console"], "level": "WARNING"},
        "django_tasks_db": {"handlers": ["console"], "level": "WARNING"},
    },
}
