"""
Soak settings: django-ox on PostgreSQL 16 (container ox-pg, port 54329).

LOCK_TIMEOUT is deliberately short (15 s against the 300 s default) so
kill-and-reclaim cycles fit inside a scenario, and backoff is compressed so
retries drain quickly at scenario end. Every task body in soaksite.tasks
sleeps for less than LOCK_TIMEOUT, preserving the documented sizing rule
(a live task must never outlive its lock).
"""

SECRET_KEY = "soak-only"
USE_TZ = True
DEBUG = False
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

INSTALLED_APPS = [
    "django_ox",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "soak_ox",
        "USER": "postgres",
        "PASSWORD": "ox",
        "HOST": "127.0.0.1",
        "PORT": "54329",
    }
}

TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "OPTIONS": {
            "MAX_ATTEMPTS": 3,
            "LOCK_TIMEOUT": 15.0,
            "BACKOFF_INITIAL": 2.0,
            "BACKOFF_MAX": 10.0,
        },
    }
}

# WARNING keeps per-task INFO chatter out of the worker logs while retry,
# reclaim, and failure records (all WARNING or ERROR) still land in them;
# those are the chaos evidence.
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "loggers": {
        "django_ox": {"handlers": ["console"], "level": "WARNING"},
    },
}
