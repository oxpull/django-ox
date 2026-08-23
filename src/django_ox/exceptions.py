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
    Recorded against an attempt that ran past its task timeout.

    The worker records it; task code never sees it raised. Python cannot
    stop a thread, so the thread running the task keeps going until the
    task returns, and whatever it returns is then discarded. The traceback
    text names the timeout and says the thread was left running.
    """

    def __init__(self, message: str, *, timeout: float) -> None:
        super().__init__(message)
        self.timeout = timeout
