from typing import TYPE_CHECKING, Any

from .timeouts import deadline, remaining

if TYPE_CHECKING:
    from .tasks import BackoffCallback, PolicyTask

__all__ = ["BackoffCallback", "PolicyTask", "__version__", "deadline", "remaining"]
__version__ = "1.5.0"


def __getattr__(name: str) -> Any:
    # Imported on first use rather than here: django_ox.tasks imports the
    # Tasks framework, and importing the package (which Django does for the
    # app before anything else) must not pull the framework in with it.
    if name in ("BackoffCallback", "PolicyTask"):
        from . import tasks

        return getattr(tasks, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
