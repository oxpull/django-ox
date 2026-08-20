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
