from .settings_postgres import *  # noqa: F403

# PostgreSQL through Django's connection pool, at psycopg_pool's defaults, on
# both aliases. Worker threads, the reconciler and the enqueue path in one
# process then share a pool, as they do in a project that turns it on.
for _alias in ("default", "replica"):
    DATABASES[_alias]["OPTIONS"] = {"pool": True}  # noqa: F405
