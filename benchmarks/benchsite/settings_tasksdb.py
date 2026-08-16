"""Benchmark settings: django-tasks-db backend on PostgreSQL 16 (container ox-bench).

Configured per the django-tasks-db 0.12.0 README (its packaged METADATA):
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

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "loggers": {
        "django_tasks": {"handlers": ["console"], "level": "WARNING"},
        "django_tasks_db": {"handlers": ["console"], "level": "WARNING"},
    },
}
