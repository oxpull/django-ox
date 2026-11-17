"""
Settings for the KEDA example. One image runs the migrate Job, the web
Deployment that serves /ox/metrics, and the worker Deployment.

Everything that differs between them comes from the environment.
"""

import os

SECRET_KEY = os.environ["DJANGO_SECRET_KEY"]
DEBUG = False
ALLOWED_HOSTS = os.environ.get("ALLOWED_HOSTS", "localhost").split(",")

INSTALLED_APPS = ["django_ox", "demo"]
ROOT_URLCONF = "demo.urls"
WSGI_APPLICATION = "demo.wsgi.application"
USE_TZ = True

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": os.environ["DB_HOST"],
        "NAME": os.environ["DB_NAME"],
        "USER": os.environ["DB_USER"],
        "PASSWORD": os.environ["DB_PASSWORD"],
        # The worker Deployment sets 600 so a worker keeps its connections
        # between tasks. Web and one-off commands keep Django's default of 0.
        "CONN_MAX_AGE": int(os.environ.get("CONN_MAX_AGE", "0")),
    }
}

TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "QUEUES": ["default", "reports"],
    }
}
