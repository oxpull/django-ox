import time

from django.tasks import task


@task(queue_name="reports")
def build_report(seconds: float) -> str:
    """Stand-in for slow work: long enough to be running when a pod stops."""
    time.sleep(seconds)
    return f"slept {seconds}s"
