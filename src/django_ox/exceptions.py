from collections.abc import Sequence
from functools import partial
from typing import Any


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
    timeout expires (the task's own, else its queue's TASK_TIMEOUTS entry,
    else TASK_TIMEOUT), so ``finally`` blocks run and an open
    ``transaction.atomic()`` rolls back on the way out. A task may catch it
    to clean up and then re-raise; a task that swallows it has its attempt
    recorded as whatever it goes on to do, provided it returns or raises
    within TASK_TIMEOUT_GRACE. An async task is cancelled instead, and
    sees ``asyncio.CancelledError`` at the await it was on; the worker
    records the cancellation as a TaskTimeout.

    It subclasses TimeoutError so that code already written for one
    treats it as one. The worker raises it with no arguments (an injected
    exception is raised by class), then fills in the message and
    ``timeout`` when it records the attempt.
    """

    def __init__(self, message: str = "", *, timeout: float | None = None) -> None:
        super().__init__(message)
        self.timeout = timeout


class StoredValueUnreadable(Exception):
    """
    A row django-ox stores holds a value that cannot be read back.

    Only a value written around django-ox's own write functions gets there:
    by SQL, a fixture or a data migration. PostgreSQL keeps a timestamp past
    year 9999 and 'infinity', SQLite keeps text in an integer column and a
    date that does not exist, MySQL keeps a zero date outside strict mode.
    Django's own read of such a row raises a different exception on each
    database, and on SQLite can read some of them as something else without
    raising at all, so django-ox reads these rows itself and raises this
    where it cannot go on without the value.

    ``alias`` is the database the row was read from, ``model`` the model's
    label, ``pk`` the row's primary key, ``fields`` the names of the fields
    that did not read (empty when the read could not say which), and
    ``reason`` what was wrong with them, safe to print. The exception the
    read itself raised, where there was one, is the ``__cause__``.
    """

    def __init__(
        self,
        *,
        alias: str = "",
        model: str = "",
        pk: Any = None,
        fields: Sequence[str] = (),
        reason: str = "",
    ) -> None:
        self.alias = alias
        self.model = model
        self.pk = pk
        self.fields = tuple(fields)
        self.reason = reason
        super().__init__(
            f"The {model} row with primary key {pk!r} in database {alias!r} "
            f"holds a value that cannot be read: {reason}"
        )

    def __reduce__(self) -> tuple[Any, ...]:
        # The arguments are keyword-only, which the default reduction, a
        # call with self.args, cannot pass.
        return (
            partial(
                type(self),
                alias=self.alias,
                model=self.model,
                pk=self.pk,
                fields=self.fields,
                reason=self.reason,
            ),
            (),
        )
