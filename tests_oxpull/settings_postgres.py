import os

from .settings import *  # noqa: F403

# Mirrors django-ox's tests/settings_postgres.py so the same disposable
# server serves both suites:
#   docker run -d --name ox-pg -e POSTGRES_PASSWORD=ox -p 54329:5432 postgres:16
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "ox",
        "HOST": "127.0.0.1",
        "PORT": "54329",
    },
    "replica": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "ox",
        "HOST": "127.0.0.1",
        "PORT": "54329",
        "TEST": {"NAME": "test_ox_replica"},
    },
}

# Scoped to OX_TEST_DB_NAME exactly as tests_oxpull/settings.py is, and for the
# reason given there.
if "OX_TEST_DB_NAME" in os.environ:
    DATABASES["default"]["NAME"] = os.environ["OX_TEST_DB_NAME"]
    DATABASES["default"].setdefault("TEST", {})["NAME"] = os.environ["OX_TEST_DB_NAME"]
    DATABASES["replica"]["TEST"]["NAME"] = os.environ["OX_TEST_DB_NAME"] + "_replica"
