from .settings_postgres import *  # noqa: F403

# PostgreSQL through Django's connection pool, at psycopg_pool's defaults.
# Every worker process the suite starts inherits it, and each has a pool of
# its own that its poll loop, lease renewal and task threads draw on. A worker
# whose threads can take the whole pool starves the rest of them, and only a
# pooled run can see that.
DATABASES["default"]["OPTIONS"] = {"pool": True}  # noqa: F405
