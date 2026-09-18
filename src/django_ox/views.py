"""
The metrics endpoint. Mount django_ox.urls to expose it.

No authentication is built in. Wrap the view, or the include, in the
project's own policy; the Monitoring page shows how.
"""

from __future__ import annotations

from django.http import HttpRequest, HttpResponse
from django.views.decorators.http import require_safe

from . import metrics as exposition

__all__ = ["metrics"]


@require_safe
def metrics(request: HttpRequest, using: str | None = None) -> HttpResponse:
    """
    The queue metrics in Prometheus text format, or OpenMetrics when the
    scraper asks for it in the Accept header. GET and HEAD only.

    ``using`` names the database alias to read. Left out, the scrape reads
    the alias the task rows are written to, which is the queue the workers
    are running. Name another where the view is mounted to serve scrapes
    from a replica and keep them off the primary::

        path("ox/metrics", metrics, {"using": "replica"})

    From the URLconf rather than from the request, because a query
    parameter would let whoever scrapes choose the database.
    """
    accept = request.headers.get("Accept", "")
    if "application/openmetrics-text" in accept:
        return HttpResponse(
            exposition.render_openmetrics(using=using),
            content_type=exposition.CONTENT_TYPE_OPENMETRICS,
        )
    return HttpResponse(
        exposition.render_prometheus(using=using),
        content_type=exposition.CONTENT_TYPE_PROMETHEUS,
    )
