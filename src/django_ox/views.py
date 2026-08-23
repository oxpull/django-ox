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
def metrics(request: HttpRequest) -> HttpResponse:
    """
    The queue metrics in Prometheus text format, or OpenMetrics when the
    scraper asks for it in the Accept header. GET and HEAD only.
    """
    accept = request.headers.get("Accept", "")
    if "application/openmetrics-text" in accept:
        return HttpResponse(
            exposition.render_openmetrics(),
            content_type=exposition.CONTENT_TYPE_OPENMETRICS,
        )
    return HttpResponse(
        exposition.render_prometheus(),
        content_type=exposition.CONTENT_TYPE_PROMETHEUS,
    )
