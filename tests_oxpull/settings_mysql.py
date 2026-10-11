import os

from .settings import *  # noqa: F403

# Mirrors django-ox's tests/settings_mysql.py so the same disposable
# server serves both suites:
#   docker run -d --name ox-mysql -e MYSQL_ROOT_PASSWORD=ox \
#       -e MYSQL_DATABASE=oxtest -p 33069:3306 mysql:8
#
# Django's MySQL backend wants mysqlclient; PyMySQL works once registered
# under the same name.
try:  # pragma: no cover - environment shim, not behaviour
    import MySQLdb  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover
    try:
        import pymysql

        pymysql.install_as_MySQLdb()
    except ModuleNotFoundError:
        pass

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "oxtest",
        "USER": "root",
        "PASSWORD": "ox",
        "HOST": "127.0.0.1",
        "PORT": "33069",
        "OPTIONS": {
            "init_command": "SET sql_mode='STRICT_TRANS_TABLES'",
            "charset": "utf8mb4",
        },
        "TEST": {"CHARSET": "utf8mb4", "COLLATION": "utf8mb4_unicode_ci"},
    },
    "replica": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "oxtest",
        "USER": "root",
        "PASSWORD": "ox",
        "HOST": "127.0.0.1",
        "PORT": "33069",
        "OPTIONS": {
            "init_command": "SET sql_mode='STRICT_TRANS_TABLES'",
            "charset": "utf8mb4",
        },
        "TEST": {
            "NAME": "test_ox_replica",
            "CHARSET": "utf8mb4",
            "COLLATION": "utf8mb4_unicode_ci",
        },
    },
}

# Scoped to OX_TEST_DB_NAME exactly as tests_oxpull/settings.py is, and for the
# reason given there.
if "OX_TEST_DB_NAME" in os.environ:
    DATABASES["default"]["NAME"] = os.environ["OX_TEST_DB_NAME"]
    DATABASES["default"].setdefault("TEST", {})["NAME"] = os.environ["OX_TEST_DB_NAME"]
    DATABASES["replica"]["TEST"]["NAME"] = os.environ["OX_TEST_DB_NAME"] + "_replica"
