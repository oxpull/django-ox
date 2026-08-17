from .settings import *  # noqa: F403

# Local verification settings for the SELECT ... FOR UPDATE SKIP LOCKED path.
# Expects a disposable server, e.g.:
#   docker run -d --name ox-pg -e POSTGRES_PASSWORD=ox -p 54329:5432 postgres:16
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "ox",
        "HOST": "127.0.0.1",
        "PORT": "54329",
    }
}
