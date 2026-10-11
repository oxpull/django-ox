"""
Where the oxpull tables live.

The uniqueness lock and the batch tables are written in the same
transaction as a django_ox row (the lock beside its task, the membership
row beside its task, the completed batch beside the callbacks it
enqueues), and the batch completion query joins membership to the task
table. Neither a transaction nor a join crosses a database alias, so
those tables must sit on the same database as the django_ox tables.

The rate window is neither co-written nor joined, and it belongs here
for the other reason: OxpullWorker issues every window read and write
against this alias, so a router that names a different database for
OxpullRateWindow is not the one the worker follows.

A router that splits any oxpull table from the django_ox tables is a
configuration error, caught at startup and repeated as a system check,
rather than a runtime fallback that loses atomicity quietly.

The alias is resolved from db_for_write and every read on these paths is
pinned to it: a router that sends reads to a lagged replica must not be
able to answer "this lock is free", "nothing is pending" or "this window
has room" from a stale snapshot.

The router is asked once, per model and without hints. django-ox saves
each task row through the same router with the row as the instance hint,
so a router that routes by model answers the same both times and the
task row lands on this alias. A router that answers differently with the
hint, one database per backend_name say, writes the task row on a
database this alias never sees: the lock row and the membership row then
commit apart from their task, and an add() that reissues its transaction
after a deadlock enqueues again on the other database. That router is
outside what this check can see, and it is not supported.
"""

from __future__ import annotations

from django.core.exceptions import ImproperlyConfigured
from django.db import models, router

# Why each table has to answer to the same alias as OxTask. The message
# carries the reason for the model that is actually split, so an operator
# reading it gets the constraint that applies rather than the union.
_CO_WRITTEN = (
    "Its rows are written in the same transaction as django_ox rows and "
    "joined to them in one query, and neither a transaction nor a join "
    "crosses a database alias."
)
# The edge table is read inside the worker's claim statement, against the
# task table, in one query. A router that sends it elsewhere would make
# the claim reference a table that is not on the connection running it.
_EDGE = (
    "The worker's claim statement reads it in the same query as the task "
    "table, and a query does not cross a database alias."
)
_PINNED = (
    "OxpullWorker reads and writes the rate window on the database that "
    "holds the task tables, so this route is not the one it follows."
)


def oxpull_db_alias() -> str:
    """
    The database holding both the django_ox and the oxpull tables.

    Raises ImproperlyConfigured if the active routers do not write all of
    them to one alias.
    """
    from django_ox.models import OxTask

    from .models import (
        OxpullBatch,
        OxpullBatchMember,
        OxpullRateWindow,
        OxpullUniqueLock,
        OxpullWorkflow,
        OxpullWorkflowEdge,
        OxpullWorkflowNode,
    )

    required: tuple[tuple[type[models.Model], str], ...] = (
        (OxpullUniqueLock, _CO_WRITTEN),
        (OxpullBatch, _CO_WRITTEN),
        (OxpullBatchMember, _CO_WRITTEN),
        (OxpullWorkflow, _CO_WRITTEN),
        (OxpullWorkflowNode, _CO_WRITTEN),
        (OxpullWorkflowEdge, _EDGE),
        (OxpullRateWindow, _PINNED),
    )
    task_db = router.db_for_write(OxTask)
    for model, reason in required:
        model_db = router.db_for_write(model)
        if model_db != task_db:
            raise ImproperlyConfigured(
                f"oxpull needs the django_ox and oxpull tables on one "
                f"database. The router writes OxTask to {task_db!r} and "
                f"{model.__name__} to {model_db!r}. {reason}"
            )
    return str(task_db)
