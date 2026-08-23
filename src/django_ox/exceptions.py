class TaskAbandoned(Exception):
    """
    Recorded against a task whose worker stopped renewing its lease with no
    attempts remaining. Never raised by task code; exists so the error
    record resolves to a real exception class via TaskError.exception_class.

    It is a note about the lease, not a diagnosis of the work. The reaper
    that records it has seen only that the claim went quiet, and the task
    may have finished, failed, or never got that far. The traceback text
    says so; nothing here should be read as a cause of failure.
    """


class TaskTimeout(TimeoutError):
    """
    Raised inside a task that ran past its timeout, and recorded against
    the attempt.

    The worker raises it on the task's own thread when the attempt's
    TASK_TIMEOUT expires, so ``finally`` blocks run and an open
    ``transaction.atomic()`` rolls back on the way out. A task may catch it
    to clean up and then re-raise; a task that swallows it has its attempt
    recorded as whatever it goes on to do. An async task sees it as the
    result of its coroutine being cancelled.

    It subclasses TimeoutError so that code already written for one
    treats it as one. The worker raises it with no arguments (an injected
    exception is raised by class), then fills in the message and
    ``timeout`` when it records the attempt.
    """

    def __init__(self, message: str = "", *, timeout: float | None = None) -> None:
        super().__init__(message)
        self.timeout = timeout
