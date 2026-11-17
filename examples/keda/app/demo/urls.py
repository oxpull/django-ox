from django.urls import include, path

# This Deployment exists to serve the metrics endpoint inside the cluster.
# A NetworkPolicy admits only Prometheus to it; see
# k8s/55-network-policy.yaml.
urlpatterns = [path("ox/", include("django_ox.urls"))]  # GET /ox/metrics
