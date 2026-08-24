"""
URL patterns for the metrics endpoint.

    path("ox/", include("django_ox.urls"))

exposes GET /ox/metrics, reversible as "django_ox:metrics".
"""

from django.urls import path

from . import views

app_name = "django_ox"

urlpatterns = [path("metrics", views.metrics, name="metrics")]
