import os

from .settings import *  # noqa: F403

# MySQL 8 verification settings. MySQL 8 supports SELECT ... FOR UPDATE SKIP
# LOCKED, so the worker takes the same claim path it uses on PostgreSQL's
# non-single-statement branch rather than the compare-and-set fallback.
#
# Expects a disposable server, e.g.:
#   docker run -d --name ox-mysql -e MYSQL_ROOT_PASSWORD=ox \
#       -e MYSQL_DATABASE=oxtest -p 33069:3306 mysql:8
#
# Django's MySQL backend wants mysqlclient. PyMySQL works too once registered
# under the same name, which avoids needing a compiler and the MySQL client
# headers just to run the suite locally.
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
            # Without STRICT_TRANS_TABLES, MySQL silently truncates rather than
            # raising, which would hide real bugs behind passing tests.
            "init_command": "SET sql_mode='STRICT_TRANS_TABLES'",
            "charset": "utf8mb4",
        },
        "TEST": {"CHARSET": "utf8mb4", "COLLATION": "utf8mb4_unicode_ci"},
    }
}

# Worker processes started from the test suite must hit the test database the
# parent created, not the development one; the parent passes its name through.
if "OX_TEST_DB_NAME" in os.environ:
    DATABASES["default"]["NAME"] = os.environ["OX_TEST_DB_NAME"]
