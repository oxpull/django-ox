"""
The router contract: django-ox never reads its own rows on a replica.

`tests/test_replica_reads.py` names two symptoms and proves them fixed. This
file asks the harder question behind them, which searching the source cannot
answer: is the sweep complete? A search sees `OxTask.objects`. It does not see
`refresh_from_db()`, a model's unique validation, a form's `_post_clean`, a
related descriptor, a manager, or a `ModelAdmin` method it does not own, and
every one of those consults `db_for_read`.

So the question is asked at the only place that sees all of them: the router.

The contract. Under `ContractRouter`, `db_for_read` for a `django_ox` model
raises `ReplicaRead` when the code that asked is django-ox's own, unless that
call site is named in ALLOWED_REPLICA_READS below with a reason. The raise
happens at the routing decision, so the traceback names the read rather than
the statement it became. Reads from a project's own code, and from Django
machinery django-ox does not own, are routed to the replica and left alone:
that is what a replica is for.

"django-ox's own code" is decided two ways, because one is not enough.

1.  A frame whose file is inside the package. Innermost first, so the site
    reported is the line to change, not the entry point above it.
2.  Failing that, a frame whose `self` or `cls` belongs to a class the
    package defines. `BaseModelForm._post_clean` runs in Django's file, but
    when `self` is `OxScheduleForm` the read is django-ox's to answer for.
    Rule 1 alone would call the duplicate-name check somebody else's problem.

Three assertions per operation, not one. Where the reads went is what the
router decides. That the operation returned the right answer is asserted
against the primary afterwards, because a read pinned to the wrong alias and
a read pinned to none are equally green if nobody looks at the result. That
it terminated is `statement_budget`, which turns the non-terminating prune
loop into a failed assertion in milliseconds rather than a hung suite.

The second harness is `LaggingReplicaRouter`: the replica router from
Django's own documentation over a replica seeded once and never again, which
is a read replica that is up and behind. It records nothing and refuses
nothing. It is here to show what the reads the contract catches actually do
to a person, and to hold that shape once they are fixed.

The admin is driven over HTTP as well as through `save_model`, the form and
the actions. Driving the methods alone is what let three defects through: both
`get_queryset` methods sat on the allow-list as reads that never fire, and
they fire on every admin page a person opens. A request costs the attribution
precision, not detection. `changelist_view` and `add_view` are django-ox's own
overrides, so rule 1 reports every read underneath them at that one line; a
coarse name for a read that must not happen is still a failure, and nothing
under those views is allowed to route.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import sys
import traceback
import uuid
from datetime import timedelta
from io import StringIO
from pathlib import Path
from types import FrameType

import pytest
from django import forms
from django.contrib.admin.helpers import ACTION_CHECKBOX_NAME
from django.contrib.admin.sites import AdminSite
from django.contrib.auth.models import Permission, User
from django.contrib.messages import get_messages
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import IntegrityError, connections
from django.test import RequestFactory
from django.urls import reverse
from django.utils import timezone

import django_ox
from django_ox import actions, metrics, stats, stored
from django_ox import admin as ox_admin
from django_ox.models import OxSchedule, OxScheduleChange, OxScheduleTick, OxTask
from django_ox.registry import ArgsForm, ScheduleKind, register
from django_ox.schedules import STORED_KEY_PREFIX
from django_ox.worker import Worker

from . import tasks

# The replica is `default` and the primary is `alt`, the way
# tests/test_replica_reads.py and tests/test_write_routing.py have them, so a
# statement that routes itself lands somewhere a pinned one does not. Both
# aliases are migrated: a misrouted read answers from the wrong database
# rather than failing on a table that is not there, which is the whole
# difficulty being reproduced.
REPLICA = "default"
PRIMARY = "alt"

OX_DIR = str(Path(django_ox.__file__).resolve().parent)
OX_PREFIX = OX_DIR + os.sep


class ReplicaRead(BaseException):
    """
    A read django-ox owns was routed to the replica.

    Derived from BaseException on purpose. django-ox catches `Exception`
    around a dispatch pass, a schedule heal and a worker's poll, so an
    `Exception` here would be swallowed by the very code being audited and
    the test would pass.
    """

    def __init__(self, model: type, site: str, chain: list[str]) -> None:
        super().__init__(
            f"{model.__name__} was read on the replica by {site}. "
            f"django-ox call chain, innermost first: {' <- '.join(chain) or 'none'}. "
            "Pin the read to the alias the write goes to, or name the site in "
            "ALLOWED_REPLICA_READS with the reason it belongs on a replica."
        )
        self.model = model
        self.site = site
        self.chain = chain


#: Reads that legitimately follow `db_for_read`, each with the reason.
#:
#: Empty, and that is the finding rather than an omission. It held one entry,
#: the schedule changelist's **Last tick** column, whose reason was that Django
#: routes the rest of that page through `db_for_read` anyway and pinning one
#: column would make one page answer from two databases. Driving the admin over
#: HTTP showed what that reasoning rested on: the page's own queryset was
#: routed, and the change form built from it submitted the replica's values
#: back over the primary. Both `get_queryset` methods are pinned now, so the
#: rest of the page comes from the primary and the column has to come from
#: there too. `test_the_allow_list_is_consulted_and_records_what_it_lets_through`
#: exercises this branch with a synthetic entry, because an allow-list that is
#: empty and an allow-list that does not work look the same from outside.
ALLOWED_REPLICA_READS: dict[str, str] = {}

#: What the contract does not reach, stated rather than left to be discovered.
#:
#: A read with no django-ox frame on the stack under either rule cannot be
#: attributed, so the router does not refuse it. Nothing in the shipped admin
#: is in that position any more: every page's queryset starts at a
#: `get_queryset` the package defines and carries an alias from there, so it
#: never consults `db_for_read` at all.
#: `test_the_admin_queryset_is_pinned_rather_than_routed` holds that, and
#: `test_a_project_s_own_read_is_left_on_the_replica` holds the other side:
#: a project reading the task table itself still gets its replica.


def _ox_site(frame: FrameType) -> str | None:
    """This frame's identity if it is django-ox's own code, else None."""
    filename = frame.f_code.co_filename
    if filename.startswith(OX_PREFIX):
        relative = os.path.relpath(filename, OX_DIR)
        return f"{relative}:{frame.f_code.co_name}"
    return None


def _ox_owner(frame: FrameType) -> str | None:
    """
    This frame's identity if it runs on behalf of a class django-ox defines.

    `BaseModelForm._post_clean` and `ModelAdmin.get_object` are Django's
    files running django-ox's classes. The package answers for what they
    read, so they have to be attributable.
    """
    bound = frame.f_locals.get("self", frame.f_locals.get("cls"))
    if bound is None:
        return None
    owner = bound if isinstance(bound, type) else type(bound)
    for base in getattr(owner, "__mro__", ()):
        module = sys.modules.get(base.__module__)
        path = getattr(module, "__file__", None)
        if path and str(Path(path).resolve()).startswith(OX_PREFIX):
            return f"{owner.__name__}.{frame.f_code.co_name}"
    return None


def ox_call_chain() -> list[str]:
    """
    Every django-ox site on the stack, innermost first.

    Rule 1 across the whole stack before rule 2, so a read made from a line
    of the package is reported at that line rather than at the Django method
    it happens to pass through. A chain that rule 1 finds nothing for falls
    back to rule 2 entirely.
    """
    frame: FrameType | None = sys._getframe(1)
    by_file: list[str] = []
    by_owner: list[str] = []
    while frame is not None:
        found = _ox_site(frame)
        if found is not None:
            by_file.append(found)
        else:
            owned = _ox_owner(frame)
            if owned is not None:
                by_owner.append(owned)
        frame = frame.f_back
    return by_file or by_owner


class ContractRouter:
    """
    Reads go to the replica; a read django-ox owns is the contract's business.

    In the default mode the read raises, at the routing decision, so the
    traceback names it. In `refuse=False` the read is recorded and answered
    with the primary instead, so one run can inventory every site rather than
    stopping at the first: that mode is what answers "is the sweep complete",
    and the per-operation tests are what refuse each one.

    Armed per operation rather than for the whole test, so a test can put rows
    where it wants them before the contract applies.
    """

    def __init__(self, *, refuse: bool = True) -> None:
        self.refuse = refuse
        self.armed = False
        self.allowed: list[tuple[str, str]] = []
        self.offences: dict[str, str] = {}

    def db_for_read(self, model: type, **hints: object) -> str | None:
        if model._meta.app_label != "django_ox":
            return None
        if self.armed:
            chain = ox_call_chain()
            if chain:
                site = chain[0]
                if site not in ALLOWED_REPLICA_READS:
                    if self.refuse:
                        raise ReplicaRead(model, site, chain)
                    where = _reading_line(chain)
                    self.offences.setdefault(where, f"{model.__name__}, from {site}")
                    return PRIMARY
                self.allowed.append((model.__name__, site))
        return PRIMARY if not self.refuse else REPLICA

    def db_for_write(self, model: type, **hints: object) -> str | None:
        return PRIMARY if model._meta.app_label == "django_ox" else None

    def allow_relation(self, obj1: object, obj2: object, **hints: object) -> bool:
        return True

    def allow_migrate(self, db: str, app_label: str, **hints: object) -> None:
        return None


def _reading_line(chain: list[str]) -> str:
    """
    The package line that made the read, for the inventory's message.

    A read rule 2 found has no package line at all: it is Django's file
    running django-ox's class, so the chain is what there is to name.
    """
    frames = [
        f"{os.path.relpath(f.filename, OX_DIR)}:{f.lineno}  {(f.line or '').strip()}"
        for f in traceback.extract_stack()
        if f.filename.startswith(OX_PREFIX)
    ]
    return frames[-1] if frames else " <- ".join(chain[:4])


class LaggingReplicaRouter:
    """The replica router from Django's own multi-database documentation."""

    def db_for_read(self, model: type, **hints: object) -> str | None:
        return REPLICA if model._meta.app_label == "django_ox" else None

    def db_for_write(self, model: type, **hints: object) -> str | None:
        return PRIMARY if model._meta.app_label == "django_ox" else None

    def allow_relation(self, obj1: object, obj2: object, **hints: object) -> bool:
        return True

    def allow_migrate(self, db: str, app_label: str, **hints: object) -> None:
        return None


@contextlib.contextmanager
def statement_budget(limit: int):
    """
    Let the two aliases serve `limit` statements between them, then fail.

    This is the termination assertion. The defect this file exists for is a
    loop whose exit condition is a read of a database the deletes do not
    reach, and waiting for it to finish would hang the suite rather than
    fail it. Covers the calling thread's connections only, which is every
    operation here except the worker's pool.
    """
    spent = 0

    def count(execute, sql, params, many, context):
        nonlocal spent
        spent += 1
        if spent > limit:
            raise AssertionError(
                f"more than {limit} statements; the one over was {sql}"
            )
        return execute(sql, params, many, context)

    with contextlib.ExitStack() as stack:
        for alias in (REPLICA, PRIMARY):
            stack.enter_context(connections[alias].execute_wrapper(count))
        yield


def a_task(**over) -> OxTask:
    fields = {
        "id": uuid.uuid4(),
        "task_path": "tests.tasks.add",
        "backend_name": "default",
        "queue_name": "default",
        "enqueued_at": timezone.now(),
        "status": OxTask.Status.READY,
        "args": [1, 2],
        "kwargs": {},
    }
    fields.update(over)
    return OxTask.objects.using(PRIMARY).create(**fields)


def finished_long_ago(n: int, **over) -> list[OxTask]:
    old = timezone.now() - timedelta(days=30)
    fields = {
        "status": OxTask.Status.SUCCESSFUL,
        "enqueued_at": old,
        "finished_at": old,
    }
    fields.update(over)
    return [a_task(**fields) for _ in range(n)]


def backlog(n: int) -> list[OxTask]:
    old = timezone.now() - timedelta(hours=6)
    return [a_task(status=OxTask.Status.READY, enqueued_at=old) for _ in range(n)]


class _Args(ArgsForm):
    region = forms.CharField()


@pytest.fixture
def registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.add))
    register(ScheduleKind(key="checked", task=tasks.add, form=_Args))


def schedule_fields(**over) -> dict[str, object]:
    fields = {
        "name": "nightly",
        "task_key": "report",
        "trigger": "cron",
        "cron": "0 2 * * *",
    }
    fields.update(over)
    return fields


SCHEDULES = "admin:django_ox_oxschedule_changelist"
SCHEDULE_ADD = "admin:django_ox_oxschedule_add"
SCHEDULE_CHANGE = "admin:django_ox_oxschedule_change"
SCHEDULE_HISTORY = "admin:django_ox_oxschedule_history"
SCHEDULE_DELETE = "admin:django_ox_oxschedule_delete"
TASKS = "admin:django_ox_oxtask_changelist"
TASK_CHANGE = "admin:django_ox_oxtask_change"
TASK_HISTORY = "admin:django_ox_oxtask_history"
AUTOCOMPLETE = "admin:autocomplete"

#: What another app's autocomplete widget sends to reach `OxTaskAdmin`.
#:
#: The endpoint takes the referring model and field, finds the model on the
#: far side of that foreign key, and asks the admin registered for it. Any
#: project field pointing at `OxTask` lands in the same place;
#: `OxScheduleTick.task` is one django-ox already ships.
TASK_AUTOCOMPLETE = {
    "app_label": "django_ox",
    "model_name": "oxscheduletick",
    "field_name": "task",
}

#: An admin page runs far more statements than a command does, between the
#: session, the user, the permissions and the page's own queries. The budget
#: is still here to catch a loop that does not end, so it is generous rather
#: than tight.
PAGE_BUDGET = 400


def schedule_post(**over) -> dict[str, str]:
    """Every field the schedule form submits, the way a browser sends them."""
    data = {
        "name": "nightly",
        "task_key": "report",
        "trigger": "cron",
        "cron": "0 2 * * *",
        "every_seconds": "",
        "phase_seconds": "0",
        "arguments": "{}",
        "enabled": "on",
        "end_time_0": "",
        "end_time_1": "",
        "starting_deadline_seconds": "",
    }
    data.update(over)
    return data


def said(response) -> list[str]:
    """The messages the admin put in front of the person."""
    return [str(message) for message in get_messages(response.wsgi_request)]


def snapshot_to_replica() -> None:
    """
    Copy every django-ox row on the primary to the replica, once.

    What a lagging replica is: a copy taken at some moment that never
    catches up. Everything written after this call exists on the primary
    alone, which is the state a read replica is in for the whole window
    this package's read-after-write paths care about.
    """
    for model in (OxTask, OxSchedule, OxScheduleTick, OxScheduleChange):
        model.objects.using(REPLICA).all().delete()
        model.objects.using(REPLICA).bulk_create(
            list(model.objects.using(PRIMARY).all())
        )


@pytest.mark.django_db(databases=[REPLICA, PRIMARY])
class TestTheRouterContract:
    """Every read in the shipped surface, asked of the router itself."""

    @pytest.fixture(autouse=True)
    def router(self, settings):
        contract = ContractRouter()
        settings.DATABASE_ROUTERS = [contract]
        return contract

    @pytest.fixture
    def armed(self, router):
        @contextlib.contextmanager
        def arm(budget: int = 400):
            router.armed = True
            try:
                with statement_budget(budget):
                    yield router
            finally:
                router.armed = False

        return arm

    # -- ox_prune ---------------------------------------------------------

    def test_prune_reads_the_primary_and_deletes_what_it_should(self, armed):
        finished_long_ago(20)
        out = StringIO()
        with armed():
            call_command("ox_prune", "--older-than", "1d", stdout=out)
        assert "Deleted 20 " in out.getvalue()
        assert OxTask.objects.using(PRIMARY).count() == 0

    def test_prune_include_failed_reads_the_primary(self, armed):
        finished_long_ago(5)
        finished_long_ago(3, status=OxTask.Status.FAILED)
        out = StringIO()
        with armed():
            call_command(
                "ox_prune", "--older-than", "1d", "--include-failed", stdout=out
            )
        assert "Deleted 8 " in out.getvalue()
        assert OxTask.objects.using(PRIMARY).count() == 0

    def test_prune_with_a_queue_reads_the_primary(self, armed):
        finished_long_ago(4, queue_name="emails")
        finished_long_ago(6, queue_name="default")
        out = StringIO()
        with armed():
            call_command(
                "ox_prune", "--older-than", "1d", "--queue", "emails", stdout=out
            )
        assert "Deleted 4 " in out.getvalue()
        assert OxTask.objects.using(PRIMARY).count() == 6

    def test_prune_terminates_when_its_batches_go_round(self, armed):
        """
        The batch loop ends on a read that comes back empty. Read anywhere
        but the alias the DELETE lands on, it never does. The budget is what
        makes that a failure in milliseconds instead of a hung suite.
        """
        finished_long_ago(20)
        with armed(budget=300):
            call_command("ox_prune", "--older-than", "1d", "--batch-size", "3")
        assert OxTask.objects.using(PRIMARY).count() == 0

    # -- ox_health --------------------------------------------------------

    def test_health_text_reads_the_primary_backlog(self, armed):
        backlog(40)
        with armed(), pytest.raises(CommandError) as caught:
            call_command("ox_health", "--max-backlog", "5")
        assert "backlog is 40" in str(caught.value)

    def test_health_json_reads_the_primary_backlog(self, armed):
        backlog(40)
        out = StringIO()
        with armed(), pytest.raises(CommandError):
            call_command(
                "ox_health", "--max-backlog", "5", "--format", "json", stdout=out
            )
        assert '"ok": false' in out.getvalue()
        assert '"backlog": 40' in out.getvalue()

    # -- the worker -------------------------------------------------------

    def test_the_worker_claims_and_completes_on_the_primary(self, armed):
        row = a_task()
        worker = Worker(backoff_initial=0, poll_interval=0.05)
        assert worker._db_alias == PRIMARY
        with armed():
            assert worker.run_once() is True
        done = OxTask.objects.using(PRIMARY).get(pk=row.pk)
        assert done.status == OxTask.Status.SUCCESSFUL
        assert done.return_value == 3

    def test_the_skip_locked_claim_reads_the_primary(self, armed, monkeypatch):
        """
        The claim path a database with SKIP LOCKED takes, which is not the
        one SQLite takes. It re-reads the row it just claimed so the
        instance and the row agree, and that read decides what the worker
        then runs. SQLite drops `FOR UPDATE` silently, so flipping the
        feature flag reaches the branch without pretending to lock.
        """
        features = connections[PRIMARY].features
        monkeypatch.setattr(features, "has_select_for_update_skip_locked", True)
        row = a_task()
        worker = Worker(backoff_initial=0, poll_interval=0.05)
        with armed():
            claimed = worker.claim_one()
        assert claimed is not None
        assert claimed.pk == row.pk
        assert claimed.status == OxTask.Status.RUNNING
        assert claimed.attempts == 1

    def test_the_reaper_reads_the_primary(self, armed):
        stale = timezone.now() - timedelta(hours=2)
        row = a_task(
            status=OxTask.Status.RUNNING,
            locked_at=stale,
            locked_by="gone",
            attempts=1,
        )
        worker = Worker(backoff_initial=0, poll_interval=0.05, lock_timeout=60)
        with armed():
            assert worker.reap() == 1
        assert (
            OxTask.objects.using(PRIMARY).get(pk=row.pk).status == OxTask.Status.READY
        )

    # -- stored.py --------------------------------------------------------

    def test_creating_a_schedule_reads_the_primary(self, armed, registry):
        with armed():
            row = stored.create_schedule(**schedule_fields())
        assert row.pk is not None
        assert OxSchedule.objects.using(PRIMARY).get(pk=row.pk).name == "nightly"
        assert OxSchedule.objects.using(REPLICA).count() == 0

    def test_updating_a_schedule_reads_the_primary(self, armed, registry):
        row = stored.create_schedule(**schedule_fields())
        with armed():
            updated = stored.update_schedule(row, cron="0 3 * * *")
        assert updated.cron == "0 3 * * *"
        assert OxSchedule.objects.using(PRIMARY).get(pk=row.pk).cron == "0 3 * * *"

    def test_deleting_a_schedule_reads_the_primary(self, armed, registry):
        row = stored.create_schedule(**schedule_fields())
        with armed():
            stored.delete_schedule(row)
        assert OxSchedule.objects.using(PRIMARY).count() == 0

    def test_the_stored_source_reads_the_primary(self, armed, registry):
        stored.create_schedule(**schedule_fields())
        source = stored.DatabaseScheduleSource({}, "default")
        with armed():
            found = source.schedules()
        assert [s.name for s in found] == ["nightly"]

    # -- unique-name validation ------------------------------------------

    def test_unique_name_validation_reads_the_primary(self, armed, registry):
        """
        The duplicate is on the primary and nowhere else. Validated against
        any other alias it is not a duplicate, and the refusal a person
        should get becomes an IntegrityError from the INSERT.
        """
        stored.create_schedule(**schedule_fields())
        with armed(), pytest.raises(ValidationError) as caught:
            stored.create_schedule(**schedule_fields(cron="0 4 * * *"))
        assert "name" in caught.value.message_dict

    def test_the_admin_form_validates_uniqueness_against_the_primary(
        self, armed, registry
    ):
        """
        The path a person takes. `ModelForm._post_clean` runs its own unique
        check before `save_model` is ever called, in Django's file with
        django-ox's form bound to `self`.
        """
        stored.create_schedule(**schedule_fields())
        request = RequestFactory().get("/")
        request.user = _AllowedUser()
        form_class = ox_admin.OxScheduleAdmin(OxSchedule, AdminSite()).get_form(
            request, None, change=False
        )
        form = form_class(
            data={
                "name": "nightly",
                "task_key": "report",
                "trigger": "cron",
                "cron": "0 4 * * *",
                "phase_seconds": 0,
                "arguments": "{}",
                "enabled": "on",
            }
        )
        with armed():
            valid = form.is_valid()
        assert valid is False
        assert "name" in form.errors

    # -- the admin's save path -------------------------------------------

    def _admin_request(self, user=None):
        request = RequestFactory().post("/")
        request.user = user
        return request

    def test_the_admin_add_save_path_reads_the_primary(self, armed, registry):
        site = ox_admin.OxScheduleAdmin(OxSchedule, AdminSite())
        obj = OxSchedule(**schedule_fields())
        with armed():
            site.save_model(self._admin_request(), obj, form=None, change=False)
        assert obj.pk is not None
        assert obj.name == "nightly"
        stored_row = OxSchedule.objects.using(PRIMARY).get(pk=obj.pk)
        assert stored_row.cron == "0 2 * * *"

    def test_the_admin_change_save_path_reads_the_primary(self, armed, registry):
        site = ox_admin.OxScheduleAdmin(OxSchedule, AdminSite())
        row = stored.create_schedule(**schedule_fields())

        class _Form:
            fields = {"cron": None}
            cleaned_data = {"cron": "0 5 * * *"}

        with armed():
            site.save_model(self._admin_request(), row, form=_Form(), change=True)
        assert OxSchedule.objects.using(PRIMARY).get(pk=row.pk).cron == "0 5 * * *"

    def test_the_admin_enable_action_reads_the_primary(self, armed, registry):
        site = ox_admin.OxScheduleAdmin(OxSchedule, AdminSite())
        row = stored.create_schedule(**schedule_fields(enabled=False))
        request = self._admin_request(user=_AllowedUser())
        request._messages = _Messages()
        selection = OxSchedule.objects.filter(pk=row.pk)
        with armed():
            site.enable_selected(request, selection)
        assert OxSchedule.objects.using(PRIMARY).get(pk=row.pk).enabled is True

    def test_the_admin_run_once_now_action_reads_the_primary(self, armed, registry):
        site = ox_admin.OxScheduleAdmin(OxSchedule, AdminSite())
        row = stored.create_schedule(**schedule_fields())
        request = self._admin_request(user=_AllowedUser())
        request._messages = _Messages()
        selection = OxSchedule.objects.filter(pk=row.pk)
        with armed():
            site.run_once_now(request, selection)
        assert OxTask.objects.using(PRIMARY).filter(status=OxTask.Status.READY).count()

    def test_the_admin_delete_queryset_reads_the_primary(self, armed, registry):
        site = ox_admin.OxScheduleAdmin(OxSchedule, AdminSite())
        row = stored.create_schedule(**schedule_fields())
        selection = OxSchedule.objects.filter(pk=row.pk)
        with armed():
            site.delete_queryset(self._admin_request(), selection)
        assert OxSchedule.objects.using(PRIMARY).count() == 0

    # -- stats, metrics, actions -----------------------------------------

    def test_every_public_stats_function_reads_the_primary(self, armed):
        backlog(40)
        readings = {}
        with armed():
            readings["queue_stats"] = stats.queue_stats()
            readings["waiting_counts"] = stats.waiting_counts()
            readings["ready_count"] = stats.ready_count()
            readings["oldest_ready_age"] = stats.oldest_ready_age()
            readings["throughput"] = stats.throughput()
            readings["failure_rate"] = stats.failure_rate()
            readings["last_claim_age"] = stats.last_claim_age()
        # Every name the module exports that is callable was exercised.
        callables = {
            name for name in stats.__all__ if callable(getattr(stats, name, None))
        } - {"QueueStats"}
        assert callables == set(readings)
        assert readings["ready_count"] == 40
        assert [entry.ready for entry in readings["queue_stats"]] == [40]
        assert readings["oldest_ready_age"] is not None

    def test_the_metrics_view_reads_the_primary(self, armed):
        backlog(40)
        with armed():
            response = self._metrics_response()
        assert response.status_code == 200
        assert 'django_ox_ready_tasks{queue="default"} 40' in response.content.decode()

    def _metrics_response(self):
        from django_ox import views

        return views.metrics(RequestFactory().get("/metrics"))

    def test_the_openmetrics_rendering_reads_the_primary(self, armed):
        backlog(40)
        with armed():
            rendered = metrics.render_openmetrics()
        assert 'django_ox_ready_tasks{queue="default"} 40' in rendered

    def test_retry_reads_the_primary(self, armed):
        row = a_task(status=OxTask.Status.FAILED, finished_at=timezone.now())
        with armed():
            assert actions.retry(row.pk) is True
        assert (
            OxTask.objects.using(PRIMARY).get(pk=row.pk).status == OxTask.Status.READY
        )

    def test_discard_reads_the_primary(self, armed):
        row = a_task()
        with armed():
            assert actions.discard(row.pk) is True
        assert (
            OxTask.objects.using(PRIMARY).get(pk=row.pk).status
            == OxTask.Status.DISCARDED
        )

    def test_retry_many_reads_the_primary(self, armed):
        rows = [
            a_task(status=OxTask.Status.FAILED, finished_at=timezone.now())
            for _ in range(3)
        ]
        with armed():
            assert actions.retry_many([row.pk for row in rows]) == (3, 0)
        assert (
            OxTask.objects.using(PRIMARY).filter(status=OxTask.Status.READY).count()
            == 3
        )

    def test_retry_many_over_a_queryset_reads_the_primary(self, armed):
        rows = [
            a_task(status=OxTask.Status.FAILED, finished_at=timezone.now())
            for _ in range(3)
        ]
        selection = OxTask.objects.filter(pk__in=[row.pk for row in rows])
        with armed():
            assert actions.retry_many(selection) == (3, 0)

    def test_discard_many_reads_the_primary(self, armed):
        rows = [a_task() for _ in range(3)]
        with armed():
            assert actions.discard_many([row.pk for row in rows]) == (3, 0)
        assert (
            OxTask.objects.using(PRIMARY).filter(status=OxTask.Status.DISCARDED).count()
            == 3
        )

    def test_discard_many_over_a_queryset_reads_the_primary(self, armed):
        rows = [a_task() for _ in range(3)]
        selection = OxTask.objects.filter(pk__in=[row.pk for row in rows])
        with armed():
            assert actions.discard_many(selection) == (3, 0)

    def test_expire_lease_reads_the_primary(self, armed):
        row = a_task(
            status=OxTask.Status.RUNNING,
            locked_at=timezone.now(),
            locked_by="someone",
            attempts=1,
        )
        with armed():
            assert actions.expire_lease(row.pk) is True
        moved = OxTask.objects.using(PRIMARY).get(pk=row.pk)
        # The lease is expired, not the task: the holder keeps running and the
        # next reaper pass is what takes the row back.
        assert moved.lease_expires_at < timezone.now()
        assert moved.status == OxTask.Status.RUNNING

    def test_the_backend_reads_a_result_back_from_the_primary(self, armed):
        from django_ox.compat import task_backends

        row = a_task()
        with armed():
            result = task_backends["default"].get_result(str(row.pk))
        assert result.id == str(row.pk)

    # -- the contract itself ---------------------------------------------

    def test_the_contract_catches_a_read_django_ox_owns(self, armed):
        """
        A negative control. Without it a harness that never fires and a
        codebase with nothing left to find look the same. `refresh_from_db`
        runs in Django's file, so this is rule 2 doing the work: the one
        searching the source cannot do.
        """
        row = a_task()
        with armed(), pytest.raises(ReplicaRead) as caught:
            row.refresh_from_db()
        assert caught.value.site == "OxTask.refresh_from_db"

    def test_the_allow_list_is_consulted_and_records_what_it_lets_through(
        self, armed, router, monkeypatch
    ):
        """
        The other half of the control, and the reason it is synthetic.

        `ALLOWED_REPLICA_READS` is empty. An empty allow-list and an allow-list
        whose branch is broken look the same from outside, so this puts one
        entry in for the length of the test and shows what the branch does:
        the read is let through, answered by the replica, and recorded rather
        than invisible. The last assertion is what an entry costs. The object
        comes back holding the value the replica still has, which is why every
        real entry was removed.
        """
        monkeypatch.setitem(
            ALLOWED_REPLICA_READS,
            "OxTask.refresh_from_db",
            "synthetic; this test only",
        )
        row = a_task()
        snapshot_to_replica()
        OxTask.objects.using(PRIMARY).filter(pk=row.pk).update(queue_name="emails")
        with armed():
            row.refresh_from_db()
        assert ("OxTask", "OxTask.refresh_from_db") in router.allowed
        assert row.queue_name == "default"
        assert OxTask.objects.using(PRIMARY).get(pk=row.pk).queue_name == "emails"

    def test_a_project_s_own_read_is_left_on_the_replica(self, armed):
        """
        The contract is about django-ox's reads, not everybody's. A project
        that reads the task table itself gets the replica, which is what it
        configured a router for.
        """
        a_task()
        with armed():
            assert OxTask.objects.count() == 0
        assert OxTask.objects.using(PRIMARY).count() == 1

    def test_the_admin_queryset_is_pinned_rather_than_routed(self, armed, registry):
        """
        The boundary of the harness, closed rather than documented.

        A queryset `ModelAdmin` builds and a template iterates puts no
        django-ox frame on the stack, so the router cannot attribute it and
        would not refuse it. That used to be where the changelist and the
        change form sat, and it is why three defects survived a harness that
        looked complete. Both `get_queryset` methods now name an alias, so the
        queryset never asks `db_for_read` and there is nothing left for the
        contract to miss.
        """
        row = stored.create_schedule(**schedule_fields())
        task = a_task()
        request = RequestFactory().get("/")
        request.user = _AllowedUser()
        schedules = ox_admin.OxScheduleAdmin(OxSchedule, AdminSite())
        tasks_admin = ox_admin.OxTaskAdmin(OxTask, AdminSite())
        with armed():
            assert schedules.get_queryset(request).db == PRIMARY
            assert tasks_admin.get_queryset(request).db == PRIMARY
            assert [s.pk for s in schedules.get_queryset(request)] == [row.pk]
            assert [t.pk for t in tasks_admin.get_queryset(request)] == [task.pk]
        assert OxSchedule.objects.using(REPLICA).count() == 0
        assert OxTask.objects.using(REPLICA).count() == 0


@pytest.mark.django_db(databases=[REPLICA, PRIMARY])
class TestTheAdminOverHttp:
    """
    The admin driven the way a person drives it, under the contract router.

    This is the surface the first version of this file could not see. It said
    two `get_queryset` methods never fire, which was true of a harness calling
    `save_model` and the actions directly and false of every page. Three
    defects lived in that gap, all of them on stored schedules, which is
    1.2.0's headline feature.

    Every test asserts twice. That no read django-ox owns reached the replica
    is the router's answer. That the page did the right thing for the person
    is the assertion the router cannot make, and it is the one that failed
    while the contract was green.

    The replica is snapshotted and then left behind, so a row written after it
    exists on the primary alone. That is what a read replica is for the whole
    window these pages care about.
    """

    @pytest.fixture(autouse=True)
    def router(self, settings):
        contract = ContractRouter()
        settings.DATABASE_ROUTERS = [contract]
        return contract

    @pytest.fixture
    def armed(self, router):
        @contextlib.contextmanager
        def arm(budget: int = PAGE_BUDGET):
            router.armed = True
            try:
                with statement_budget(budget):
                    yield router
            finally:
                router.armed = False

        return arm

    @pytest.fixture
    def operator(self, client):
        """
        A superuser, logged in, before the contract is armed.

        The session, the user and the admin log are not django-ox's tables, so
        the router says nothing about them and the login works off `default`
        as it always did.
        """
        client.force_login(
            User.objects.create_superuser("contract", "c@example.com", "pw")
        )
        return client

    # -- the schedule changelist ------------------------------------------

    def test_the_changelist_lists_the_rows_the_workers_have(
        self, armed, registry, operator
    ):
        snapshot_to_replica()
        row = stored.create_schedule(**schedule_fields())
        with armed():
            page = operator.get(reverse(SCHEDULES))
        assert page.status_code == 200
        assert [s.pk for s in page.context["cl"].result_list] == [row.pk]
        assert OxSchedule.objects.using(REPLICA).count() == 0

    def test_the_changelist_filters_and_search_read_the_primary(
        self, armed, registry, operator
    ):
        snapshot_to_replica()
        on = stored.create_schedule(**schedule_fields(name="on"))
        stored.create_schedule(**schedule_fields(name="off", enabled=False))
        with armed():
            page = operator.get(
                reverse(SCHEDULES),
                {"enabled__exact": "1", "trigger__exact": "cron", "q": "on"},
            )
        assert page.status_code == 200
        assert [s.pk for s in page.context["cl"].result_list] == [on.pk]

    def test_the_last_tick_column_comes_from_its_row_s_database(
        self, armed, registry, operator
    ):
        """
        The column that used to be the allow-list's only entry.

        Its reason was that the rest of the page came from the replica, so
        pinning this one read would split the page across two databases. The
        rest of the page comes from the primary now, so the reasoning reverses
        and the column has to be pinned too. A tick written after the snapshot
        is on the primary alone; unpinned this column reports "never" beside a
        row that ran an hour ago.
        """
        row = stored.create_schedule(**schedule_fields())
        snapshot_to_replica()
        ran_at = timezone.now() - timedelta(hours=1)
        OxScheduleTick.objects.using(PRIMARY).create(
            schedule_name=f"{STORED_KEY_PREFIX}{row.pk}",
            scheduled_for=ran_at,
            created_at=ran_at,
        )
        with armed():
            page = operator.get(reverse(SCHEDULES))
        assert page.status_code == 200
        body = page.content.decode()
        assert f"{ran_at:%Y-%m-%d %H:%M}" in body
        # The cell itself, not the word: the page also carries the warning
        # about schedules that are "stored and never dispatched".
        assert ">never<" not in body
        assert OxScheduleTick.objects.using(REPLICA).count() == 0

    # -- the change form, and the write it feeds ---------------------------

    def test_the_change_form_renders_the_row_as_it_stands(
        self, armed, registry, operator
    ):
        row = stored.create_schedule(**schedule_fields())
        snapshot_to_replica()
        OxSchedule.objects.using(PRIMARY).filter(pk=row.pk).update(cron="0 9 * * *")
        with armed():
            page = operator.get(reverse(SCHEDULE_CHANGE, args=[row.pk]))
        assert page.status_code == 200
        assert page.context["adminform"].form.initial["cron"] == "0 9 * * *"

    def test_saving_the_change_form_cannot_write_a_stale_copy_back(
        self, armed, registry, operator
    ):
        """
        The serious one, and it is silent.

        A change form submits every field, including the ones the person did
        not touch. Rendered from a replica, Save writes the replica's values
        over the primary's newer ones. `stored.update_schedule` takes the row
        lock and writes only the submitted fields precisely to stop a caller
        holding a stale copy doing this, and the admin handed it a stale copy.
        Nothing raised and nothing was logged.

        The POST carries what the page rendered, the way a browser does, so
        the test follows the person rather than a payload written by hand.
        """
        row = stored.create_schedule(**schedule_fields())
        snapshot_to_replica()
        OxSchedule.objects.using(PRIMARY).filter(pk=row.pk).update(cron="0 9 * * *")
        url = reverse(SCHEDULE_CHANGE, args=[row.pk])
        with armed():
            page = operator.get(url)
        shown = page.context["adminform"].form.initial["cron"]
        with armed():
            saved = operator.post(
                url, schedule_post(name="renamed", cron=shown, _save="Save")
            )
        assert saved.status_code == 302
        after = OxSchedule.objects.using(PRIMARY).get(pk=row.pk)
        assert after.name == "renamed"
        assert after.cron == "0 9 * * *"

    # -- the add page, and the redirect that follows it --------------------

    def test_the_add_page_redirect_finds_the_row_it_just_wrote(
        self, armed, registry, operator
    ):
        """
        `ModelAdmin.get_object` reads `get_queryset`, so "save and continue
        editing" looked the row up on the replica that had never seen it. The
        person was told the schedule they had just made does not exist, and
        dropped on the admin index.
        """
        snapshot_to_replica()
        with armed():
            added = operator.post(
                reverse(SCHEDULE_ADD),
                schedule_post(name="fresh", _continue="Save and continue editing"),
                follow=True,
            )
        assert added.status_code == 200
        row = OxSchedule.objects.using(PRIMARY).get(name="fresh")
        assert added.redirect_chain == [(reverse(SCHEDULE_CHANGE, args=[row.pk]), 302)]
        told = said(added)
        assert any("was added successfully" in line for line in told)
        assert not any("Perhaps it was deleted" in line for line in told)

    # -- delete_selected ---------------------------------------------------

    def test_delete_selected_deletes_the_rows_and_says_so(
        self, armed, registry, operator
    ):
        """
        Django takes `len(queryset)` on the queryset `get_queryset` returned,
        and returns early when it is zero without reaching django-ox's own
        `delete_queryset`. On a replica that has not seen the rows that count
        was zero, so the action deleted nothing and emitted no message at all.
        """
        snapshot_to_replica()
        rows = [
            stored.create_schedule(**schedule_fields(name=f"s{n}")) for n in range(3)
        ]
        selected = {
            "action": "delete_selected",
            ACTION_CHECKBOX_NAME: [str(row.pk) for row in rows],
        }
        with armed():
            confirm = operator.post(reverse(SCHEDULES), selected)
        assert confirm.status_code == 200
        assert "Are you sure" in confirm.content.decode()
        with armed():
            done = operator.post(reverse(SCHEDULES), selected | {"post": "yes"})
        assert done.status_code == 302
        assert any("Successfully deleted 3" in line for line in said(done))
        assert OxSchedule.objects.using(PRIMARY).count() == 0

    # -- the task admin ----------------------------------------------------

    def test_the_task_changelist_its_filters_and_a_task_page(self, armed, operator):
        snapshot_to_replica()
        ready = [a_task() for _ in range(2)]
        failed = a_task(status=OxTask.Status.FAILED, finished_at=timezone.now())
        with armed():
            listing = operator.get(reverse(TASKS))
        assert listing.status_code == 200
        assert {t.pk for t in listing.context["cl"].result_list} == {
            row.pk for row in [*ready, failed]
        }
        with armed():
            filtered = operator.get(
                reverse(TASKS),
                {
                    "status__exact": OxTask.Status.FAILED,
                    "queue_name": "default",
                    "q": "tests.tasks.add",
                    "enqueued_at__year": str(timezone.now().year),
                },
            )
        assert [t.pk for t in filtered.context["cl"].result_list] == [failed.pk]
        with armed():
            detail = operator.get(reverse(TASK_CHANGE, args=[failed.pk]))
        assert detail.status_code == 200
        assert OxTask.objects.using(REPLICA).count() == 0

    def test_a_task_action_posted_from_the_changelist(self, armed, operator):
        snapshot_to_replica()
        failed = a_task(status=OxTask.Status.FAILED, finished_at=timezone.now())
        with armed():
            acted = operator.post(
                reverse(TASKS),
                {
                    "action": "retry_selected",
                    ACTION_CHECKBOX_NAME: [str(failed.pk)],
                },
                follow=True,
            )
        assert acted.status_code == 200
        assert any("Retried 1 task(s)." in line for line in said(acted))
        assert (
            OxTask.objects.using(PRIMARY).get(pk=failed.pk).status
            == OxTask.Status.READY
        )

    # -- the pages that are neither a changelist nor a change form --------
    #
    # A first version of this class drove the changelist, the change form,
    # the add page, the actions and `delete_selected`, and said so. That left
    # six page shapes a person reaches from those same pages, and a test that
    # names the surface it drives has to drive the surface it names.

    def test_the_history_page_and_the_delete_confirmation(
        self, armed, registry, operator
    ):
        """
        Two pages that reach a single row through `ModelAdmin.get_object`.

        `get_object` reads `get_queryset`, so both follow the pin. Unpinned,
        the history page tells the person the row was deleted and the
        confirmation page offers to delete nothing.
        """
        snapshot_to_replica()
        row = stored.create_schedule(**schedule_fields(name="historic"))
        task = a_task(status=OxTask.Status.FAILED, finished_at=timezone.now())
        with armed():
            schedule_history = operator.get(reverse(SCHEDULE_HISTORY, args=[row.pk]))
        assert schedule_history.status_code == 200
        assert "historic" in schedule_history.content.decode()
        with armed():
            task_history = operator.get(reverse(TASK_HISTORY, args=[task.pk]))
        assert task_history.status_code == 200
        delete = reverse(SCHEDULE_DELETE, args=[row.pk])
        with armed():
            confirm = operator.get(delete)
        assert confirm.status_code == 200
        assert "Are you sure" in confirm.content.decode()
        with armed():
            done = operator.post(delete, {"post": "yes"})
        assert done.status_code == 302
        assert any("was deleted successfully" in line for line in said(done))
        assert OxSchedule.objects.using(PRIMARY).filter(pk=row.pk).count() == 0
        assert OxSchedule.objects.using(REPLICA).count() == 0

    def test_the_changelist_variants_a_person_can_ask_for(
        self, armed, registry, operator
    ):
        """
        Facet counts, show-all and the raw-id popup.

        Each is the changelist under a query parameter, and each takes a
        different path through `ChangeList`: facets run extra aggregates,
        show-all drops the paginator, and the popup validates `_to_field`
        before it builds the page. All three start at the same pinned
        `get_queryset`, which is why one pin covers them.
        """
        snapshot_to_replica()
        rows = [
            stored.create_schedule(**schedule_fields(name="on")),
            stored.create_schedule(**schedule_fields(name="off", enabled=False)),
        ]
        wanted = {row.pk for row in rows}
        with armed():
            faceted = operator.get(reverse(SCHEDULES), {"_facets": "True"})
        assert faceted.status_code == 200
        # One enabled, one not, both on cron, none on an interval. Every
        # count comes from the changelist queryset, so a replica that has
        # seen none of these rows renders four zeroes.
        counts = re.findall(r"\((\d+)\)", faceted.content.decode())
        assert sorted(counts) == ["0", "1", "1", "2"]
        with armed():
            everything = operator.get(reverse(SCHEDULES), {"all": ""})
        assert everything.context["cl"].show_all is True
        assert {s.pk for s in everything.context["cl"].result_list} == wanted
        with armed():
            popup = operator.get(reverse(SCHEDULES), {"_to_field": "id", "_popup": "1"})
        assert popup.context["cl"].to_field == "id"
        assert {s.pk for s in popup.context["cl"].result_list} == wanted
        assert OxSchedule.objects.using(REPLICA).count() == 0

    def test_the_autocomplete_endpoint_another_form_calls(self, armed, operator):
        """
        `/admin/autocomplete/`, which no django-ox page opens.

        A project whose own model has a foreign key to `OxTask` and lists it
        in `autocomplete_fields` sends the browser here, and the endpoint
        answers from `OxTaskAdmin.get_queryset`. Unpinned it offers no task
        the replica has not seen, so the field cannot be filled in.
        """
        snapshot_to_replica()
        task = a_task(status=OxTask.Status.FAILED, finished_at=timezone.now())
        with armed():
            answered = operator.get(
                reverse(AUTOCOMPLETE),
                TASK_AUTOCOMPLETE | {"term": str(task.pk)[:8]},
            )
        assert answered.status_code == 200
        offered = json.loads(answered.content.decode())
        assert [choice["id"] for choice in offered["results"]] == [str(task.pk)]
        assert OxTask.objects.using(REPLICA).count() == 0


@pytest.mark.django_db(databases=[REPLICA, PRIMARY])
class TestTheInventory:
    """
    The whole surface in one run, recorded rather than refused.

    The tests above refuse one read and stop, which names a site precisely and
    hides whatever is behind it. This drives the same surface with the router
    answering the primary, so the run stays correct and every site is on the
    record at once. It is the artifact that answers "is the sweep complete":
    when it passes, the only reads django-ox makes on a replica are the ones
    ALLOWED_REPLICA_READS names and gives a reason for.
    """

    @pytest.fixture(autouse=True)
    def router(self, settings):
        recorder = ContractRouter(refuse=False)
        settings.DATABASE_ROUTERS = [recorder]
        return recorder

    def test_no_read_django_ox_owns_reaches_a_replica(
        self, router, registry, settings, client
    ):
        self._drive_every_workflow(router, settings, client)
        assert not router.offences, "\n".join(
            ["reads django-ox owns that followed db_for_read:", ""]
            + [f"  {where}\n      {what}" for where, what in router.offences.items()]
        )

    def _drive_every_workflow(self, router, settings, client):
        from django_ox import bulk, views
        from django_ox.compat import task_backends

        def armed(operation):
            router.armed = True
            return _disarm(router)

        # -- the two commands -----------------------------------------
        finished_long_ago(6)
        with armed("ox_prune"):
            call_command("ox_prune", "--older-than", "1d", "--batch-size", "2")
        finished_long_ago(3)
        finished_long_ago(2, status=OxTask.Status.FAILED)
        with armed("ox_prune --include-failed --queue"):
            call_command(
                "ox_prune",
                "--older-than",
                "1d",
                "--include-failed",
                "--queue",
                "default",
            )
        backlog(9)
        for extra in ([], ["--format", "json"]):
            with armed("ox_health"), pytest.raises(CommandError):
                call_command("ox_health", "--max-backlog", "1", *extra)

        # -- stats, metrics, the endpoint -----------------------------
        with armed("stats and metrics"):
            stats.queue_stats()
            stats.waiting_counts()
            stats.ready_count()
            stats.oldest_ready_age()
            stats.throughput()
            stats.failure_rate()
            stats.last_claim_age()
            metrics.render_prometheus()
            metrics.render_openmetrics()
            assert views.metrics(RequestFactory().get("/ox/metrics")).status_code == 200

        # -- the worker, both claim paths, and the reaper --------------
        OxTask.objects.using(PRIMARY).all().delete()
        a_task()
        with armed("worker run_once"):
            assert Worker(backoff_initial=0, poll_interval=0.05).run_once() is True
        OxTask.objects.using(PRIMARY).all().delete()
        a_task()
        features = connections[PRIMARY].features
        features.has_select_for_update_skip_locked = True
        try:
            with armed("worker claim, SKIP LOCKED path"):
                claimed = Worker(backoff_initial=0, poll_interval=0.05).claim_one()
            assert claimed is not None
            assert claimed.status == OxTask.Status.RUNNING
        finally:
            del features.has_select_for_update_skip_locked
        OxTask.objects.using(PRIMARY).all().delete()
        a_task(
            status=OxTask.Status.RUNNING,
            locked_at=timezone.now() - timedelta(hours=2),
            locked_by="gone",
            attempts=1,
        )
        with armed("reaper"):
            assert (
                Worker(backoff_initial=0, poll_interval=0.05, lock_timeout=60).reap()
                == 1
            )

        # -- enqueueing and the actions -------------------------------
        OxTask.objects.using(PRIMARY).all().delete()
        with armed("enqueue"):
            bulk.enqueue_many(tasks.add, [((1, 2), {}), ((3, 4), {})])
            tasks.add.enqueue(5, 6)
        assert OxTask.objects.using(PRIMARY).count() == 3
        OxTask.objects.using(PRIMARY).all().delete()
        rows = [
            a_task(status=OxTask.Status.FAILED, finished_at=timezone.now())
            for _ in range(3)
        ]
        ids = [row.pk for row in rows]
        with armed("actions"):
            assert actions.retry(ids[0]) is True
            assert actions.discard(ids[1]) is True
            # One FAILED row left to requeue, then two requeued rows to close.
            assert actions.retry_many(ids) == (1, 2)
            assert actions.discard_many(ids) == (2, 1)
            # All three are DISCARDED by now, so both forms change nothing.
            assert actions.retry_many(OxTask.objects.filter(pk__in=ids)) == (0, 3)
            assert actions.discard_many(OxTask.objects.filter(pk__in=ids)) == (0, 3)
            assert task_backends["default"].get_result(str(ids[0])).id == str(ids[0])

        # -- stored schedules, and the dispatch that reads them --------
        with armed("stored.create_schedule"):
            created = stored.create_schedule(**schedule_fields())
        with armed("stored.update_schedule"):
            assert stored.update_schedule(created, cron="0 3 * * *").cron == "0 3 * * *"
        with armed("the stored source"):
            source = stored.DatabaseScheduleSource({}, "default")
            assert [entry.name for entry in source.schedules()] == ["nightly"]
        stored.create_schedule(
            name="every-second",
            task_key="report",
            trigger="interval",
            every_seconds=1,
            start_time=timezone.now() - timedelta(minutes=5),
        )
        settings.TASKS = _tasks_with_stored_source()
        with armed("worker dispatch_schedules"):
            fired = Worker(backoff_initial=0, poll_interval=0.05).dispatch_schedules()
        assert fired == 1, "the dispatch pass fired nothing, so it read nothing"

        # -- the admin's writes ----------------------------------------
        site = ox_admin.OxScheduleAdmin(OxSchedule, AdminSite())
        request = RequestFactory().post("/")
        request.user = _AllowedUser()
        request._messages = _Messages()

        class _Form:
            fields = {"cron": None}
            cleaned_data = {"cron": "0 6 * * *"}

        with armed("admin save_model, change"):
            site.save_model(request, created, form=_Form(), change=True)
        added = OxSchedule(**schedule_fields(name="second"))
        with armed("admin save_model, add"):
            site.save_model(request, added, form=None, change=False)
        assert added.pk is not None
        with armed("admin enable_selected"):
            site.enable_selected(request, OxSchedule.objects.filter(pk=created.pk))
        with armed("admin disable_selected"):
            site.disable_selected(request, OxSchedule.objects.filter(pk=created.pk))
        with armed("admin run_once_now"):
            site.run_once_now(request, OxSchedule.objects.filter(pk=created.pk))
        with armed("admin delete_queryset"):
            site.delete_queryset(request, OxSchedule.objects.filter(pk=added.pk))
        with armed("admin delete_model"):
            site.delete_model(request, created)

        # -- the admin's own form, which validates before any of that ---
        get_request = RequestFactory().get("/")
        get_request.user = _AllowedUser()
        form = site.get_form(get_request, None, change=False)(
            data={
                "name": "third",
                "task_key": "report",
                "trigger": "cron",
                "cron": "0 7 * * *",
                "phase_seconds": 0,
                "arguments": "{}",
                "enabled": "on",
            }
        )
        with armed("admin form validation"):
            assert form.is_valid() is True

        # -- the task admin's actions ----------------------------------
        task_site = ox_admin.OxTaskAdmin(OxTask, AdminSite())
        with armed("OxTaskAdmin actions"):
            task_site.retry_selected(request, OxTask.objects.all())
            task_site.discard_selected(request, OxTask.objects.all())

        # -- and all of it again over HTTP, which is where a person is --
        # Calling the methods above is not the same surface. `get_queryset`,
        # `get_object` and Django's own `delete_selected` only run on a
        # request, and that is where the three admin defects were.
        self._drive_the_admin_over_http(armed, client)

    def _drive_the_admin_over_http(self, armed, client):
        client.force_login(
            User.objects.create_superuser("inventory", "i@example.com", "pw")
        )
        listed = stored.create_schedule(**schedule_fields(name="listed"))
        OxScheduleTick.objects.using(PRIMARY).create(
            schedule_name=f"{STORED_KEY_PREFIX}{listed.pk}",
            scheduled_for=timezone.now(),
            created_at=timezone.now(),
        )
        change = reverse(SCHEDULE_CHANGE, args=[listed.pk])
        with armed("admin schedule changelist"):
            assert client.get(reverse(SCHEDULES)).status_code == 200
        with armed("admin schedule changelist, filtered and searched"):
            filters = {"enabled__exact": "1", "trigger__exact": "cron", "q": "listed"}
            assert client.get(reverse(SCHEDULES), filters).status_code == 200
        with armed("admin change form, GET"):
            assert client.get(change).status_code == 200
        with armed("admin change form, POST"):
            saved = client.post(change, schedule_post(name="listed", _save="Save"))
            assert saved.status_code == 302
        with armed("admin add page, GET"):
            assert client.get(reverse(SCHEDULE_ADD)).status_code == 200
        with armed("admin add page, POST and the redirect after it"):
            added = client.post(
                reverse(SCHEDULE_ADD),
                schedule_post(name="typed-in", _continue="Save and continue editing"),
                follow=True,
            )
            assert added.redirect_chain and added.status_code == 200
        for action in ("disable_selected", "enable_selected", "run_once_now"):
            with armed(f"admin {action} over HTTP"):
                posted = client.post(
                    reverse(SCHEDULES),
                    {"action": action, ACTION_CHECKBOX_NAME: [str(listed.pk)]},
                    follow=True,
                )
                assert posted.status_code == 200
        with armed("admin schedule changelist, facet counts"):
            faceted = client.get(reverse(SCHEDULES), {"_facets": "True"})
            assert faceted.status_code == 200
        with armed("admin schedule changelist, show all"):
            assert client.get(reverse(SCHEDULES), {"all": ""}).status_code == 200
        with armed("admin schedule changelist, raw id popup"):
            popup = client.get(reverse(SCHEDULES), {"_to_field": "id", "_popup": "1"})
            assert popup.status_code == 200
        with armed("admin schedule history page"):
            history = client.get(reverse(SCHEDULE_HISTORY, args=[listed.pk]))
            assert history.status_code == 200
        selected = {
            "action": "delete_selected",
            ACTION_CHECKBOX_NAME: [str(listed.pk)],
        }
        with armed("admin delete_selected, the confirmation page"):
            assert client.post(reverse(SCHEDULES), selected).status_code == 200
        with armed("admin delete_selected, confirmed"):
            done = client.post(reverse(SCHEDULES), selected | {"post": "yes"})
            assert done.status_code == 302
        assert not OxSchedule.objects.using(PRIMARY).filter(pk=listed.pk).exists()

        single = stored.create_schedule(**schedule_fields(name="one-by-one"))
        delete = reverse(SCHEDULE_DELETE, args=[single.pk])
        with armed("admin single delete confirmation"):
            assert client.get(delete).status_code == 200
        with armed("admin single delete, confirmed"):
            assert client.post(delete, {"post": "yes"}).status_code == 302
        assert not OxSchedule.objects.using(PRIMARY).filter(pk=single.pk).exists()

        failed = a_task(status=OxTask.Status.FAILED, finished_at=timezone.now())
        with armed("admin task changelist"):
            assert client.get(reverse(TASKS)).status_code == 200
        with armed("admin task changelist, filtered, searched, by date"):
            filters = {
                "status__exact": OxTask.Status.FAILED,
                "queue_name": "default",
                "q": "tests.tasks.add",
                "enqueued_at__year": str(timezone.now().year),
            }
            assert client.get(reverse(TASKS), filters).status_code == 200
        with armed("admin task page"):
            assert client.get(reverse(TASK_CHANGE, args=[failed.pk])).status_code == 200
        with armed("admin task changelist, facet counts"):
            faceted = client.get(reverse(TASKS), {"_facets": "True"})
            assert faceted.status_code == 200
        with armed("admin task changelist, show all"):
            assert client.get(reverse(TASKS), {"all": ""}).status_code == 200
        with armed("admin task changelist, raw id popup"):
            popup = client.get(reverse(TASKS), {"_to_field": "id", "_popup": "1"})
            assert popup.status_code == 200
        with armed("admin task history page"):
            history = client.get(reverse(TASK_HISTORY, args=[failed.pk]))
            assert history.status_code == 200
        with armed("admin autocomplete endpoint for OxTask"):
            answered = client.get(
                reverse(AUTOCOMPLETE), TASK_AUTOCOMPLETE | {"term": ""}
            )
            assert answered.status_code == 200
            assert json.loads(answered.content.decode())["results"]
        with armed("admin task actions over HTTP"):
            for action in ("retry_selected", "discard_selected"):
                acted = client.post(
                    reverse(TASKS),
                    {"action": action, ACTION_CHECKBOX_NAME: [str(failed.pk)]},
                    follow=True,
                )
                assert acted.status_code == 200


@contextlib.contextmanager
def _disarm(router):
    try:
        yield
    finally:
        router.armed = False


def _tasks_with_stored_source() -> dict[str, object]:
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {
                "MAX_ATTEMPTS": 3,
                "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource",
            },
        }
    }


class _AllowedUser:
    """A user every permission check passes, without a database row."""

    is_active = True
    is_staff = True
    is_superuser = True

    def has_perm(self, perm, obj=None):
        return True


class _Messages:
    """Django's message store, reduced to what an admin action touches."""

    def __init__(self):
        self.added = []

    def add(self, level, message, extra_tags=""):
        self.added.append((level, str(message)))


@pytest.mark.django_db(databases=[REPLICA, PRIMARY])
class TestWhatALaggingReplicaDoes:
    """
    The same routing, recorded rather than refused, against a replica that
    is behind. Where the contract names a call site, this names the person's
    experience of it.
    """

    @pytest.fixture(autouse=True)
    def router(self, settings):
        settings.DATABASE_ROUTERS = [LaggingReplicaRouter()]

    def test_a_claim_hands_back_the_row_it_claimed(self):
        """
        The claim path of every database with SKIP LOCKED, which is MySQL 8
        and MariaDB 10.6 for every task they take, and PostgreSQL whenever a
        subclass's claim filter keeps the single-statement form away.

        The UPDATE that hands the row over lands on the primary. The re-read
        that follows it does not, so the worker goes on to run the task
        holding the row as it was before it claimed it: READY, no attempt
        counted, and the lease number the fencing check will refuse its
        finish write on.

        SQLite has no SKIP LOCKED and drops FOR UPDATE without a word, so
        flipping the flag reaches the branch without pretending to lock.
        """
        features = connections[PRIMARY].features
        features.has_select_for_update_skip_locked = True
        try:
            row = a_task()
            snapshot_to_replica()
            claimed = Worker(backoff_initial=0, poll_interval=0.05).claim_one()
        finally:
            del features.has_select_for_update_skip_locked
        held = OxTask.objects.using(PRIMARY).get(pk=row.pk)
        assert held.status == OxTask.Status.RUNNING
        assert claimed is not None
        assert claimed.status == held.status, (
            "the worker was handed the row as the replica still has it, so it "
            "runs the task believing it has not been claimed"
        )
        assert claimed.lease_epoch == held.lease_epoch, (
            "the worker carries a lease number the row no longer has, and its "
            "own finish write will match no rows"
        )

    def test_a_schedule_update_returns_what_was_written(self, registry):
        """
        The row existed when the replica was seeded, so the read-back finds
        it and answers with the values from before the write.
        """
        row = stored.create_schedule(**schedule_fields())
        snapshot_to_replica()
        updated = stored.update_schedule(row, cron="0 3 * * *")
        assert OxSchedule.objects.using(PRIMARY).get(pk=row.pk).cron == "0 3 * * *"
        assert updated.cron == "0 3 * * *", (
            "the update was written and the object handed back shows the old "
            "value, read from the replica"
        )

    def test_a_schedule_created_after_the_snapshot_can_be_updated(self, registry):
        """
        The same read-back, against a replica that has not seen the row at
        all, which is the ordinary case for a schedule someone just made.
        """
        snapshot_to_replica()
        row = stored.create_schedule(**schedule_fields())
        try:
            stored.update_schedule(row, cron="0 3 * * *")
        except OxSchedule.DoesNotExist:
            pytest.fail(
                "the update committed and then raised DoesNotExist reading the "
                "row back; the person is told their edit failed after it worked"
            )
        assert OxSchedule.objects.using(PRIMARY).get(pk=row.pk).cron == "0 3 * * *"

    def test_an_admin_add_survives_the_read_back(self, registry):
        snapshot_to_replica()
        site = ox_admin.OxScheduleAdmin(OxSchedule, AdminSite())
        request = RequestFactory().post("/")
        request.user = _AllowedUser()
        obj = OxSchedule(**schedule_fields())
        site.save_model(request, obj, form=None, change=False)
        assert obj.pk is not None
        assert OxSchedule.objects.using(PRIMARY).filter(pk=obj.pk).exists()

    def test_a_duplicate_name_is_refused_rather_than_raising_on_the_insert(
        self, registry
    ):
        snapshot_to_replica()
        stored.create_schedule(**schedule_fields())
        try:
            stored.create_schedule(**schedule_fields(cron="0 4 * * *"))
        except IntegrityError:
            pytest.fail(
                "the duplicate name passed validation against the replica and "
                "failed on the INSERT; the person gets a 500 where they should "
                "get a field error"
            )
        except Exception as exc:
            assert "name" in getattr(exc, "message_dict", {})
        else:
            pytest.fail("a duplicate name was accepted")

    def test_the_admin_add_page_refuses_a_duplicate_name(self, registry, client):
        snapshot_to_replica()
        stored.create_schedule(**schedule_fields())
        staff = _staff_user()
        client.force_login(staff)
        try:
            response = client.post(
                reverse("admin:django_ox_oxschedule_add"),
                {
                    "name": "nightly",
                    "task_key": "report",
                    "trigger": "cron",
                    "cron": "0 4 * * *",
                    "phase_seconds": 0,
                    "arguments": "{}",
                    "enabled": "on",
                },
            )
        except IntegrityError:
            pytest.fail(
                "the admin add page let a duplicate name through validation "
                "and raised on the INSERT"
            )
        assert response.status_code == 200
        assert "name" in response.context["adminform"].form.errors


def _staff_user() -> User:
    user = User.objects.create_user("contract", "c@example.com", "pw", is_staff=True)
    for codename in (
        "add_oxschedule",
        "change_oxschedule",
        "delete_oxschedule",
        "view_oxschedule",
    ):
        user.user_permissions.add(Permission.objects.get(codename=codename))
    return user
