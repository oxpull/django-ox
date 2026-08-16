class TaskAbandoned(Exception):
    """
    Recorded against a task when its worker lock expired and no attempts
    remain. Never raised by task code; exists so the error record resolves
    to a real exception class via TaskError.exception_class.
    """
