"""
The schedule admin's actions and "Delete selected" when a selected row does
not read.

Django hands an action the selected rows as a queryset, and iterating it
reads every one of them as a single result. One value in one row that the
driver or Django cannot convert ends that read, and an action that iterates
ends with it, for the whole selection: the row that does not read cannot be
stopped or removed, and neither can a healthy row selected beside it. On
SQLite some values read as something else instead, and an action that takes
the read at its word acts on a row that is not the one stored.

So each action takes the selected rows by key and reads them one at a time.
Disable pauses a row whatever it holds. Enable and a manual run leave a row
that does not read as it is and say which one. "Delete selected" names it
without reading it and removes it. The rows beside it are acted on as ever,
and so is a schedule whose tick history is what does not read.

Every value is written by SQL in the form the engine that keeps it keeps it,
and read back here the same way, past every converter. A case for another
engine's value skips and says why.
"""

import re

import pytest
from django.contrib.admin.models import CHANGE, DELETION, LogEntry
from django.contrib.admin.sites import site
from django.contrib.auth.models import Permission, User
from django.db import connection, connections
from django.db.models import QuerySet
from django.db.models.signals import post_delete, pre_delete
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from django_ox.admin import OxScheduleAdmin
from django_ox.models import OxSchedule, OxScheduleChange, OxTask

from .test_schedule_admin_unreadable import CHANGE as CHANGE_PAGE
from .test_schedule_admin_unreadable import (
    CHANGELIST,
    EVERY_PERMISSION,
    ONE_PER_ENGINE,
    RECOVERY_ROWS,
    SCHEDULES,
    UNREADABLE_NEWEST_TICKS,
    UNREADABLE_ROWS,
    _registry,  # noqa: F401 (a fixture)
    a_schedule,
    an_unreadable_schedule,
    body,
    by_sql,
    clock,  # noqa: F401 (a fixture)
    insert_tick,
    label,
    messages_of,
    only_on,
    set_column,
    staff,
)

CONFIRMATION = "admin/delete_selected_confirmation.html"


@pytest.fixture
def admin_user(db):
    return User.objects.create_superuser("root", "root@example.com", "pw")


@pytest.fixture(autouse=True)
def _dispatched(settings):
    # A backend that dispatches stored schedules, so that what a page says
    # is what the action said and not also that nothing dispatches them.
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"},
        }
    }


def act(client, action, pks, **more):
    """Apply a changelist action to these rows and follow it to the page after."""
    return client.post(
        reverse(CHANGELIST),
        {"action": action, "_selected_action": [str(pk) for pk in pks], **more},
        follow=True,
    )


def went_back_to_the_list(response):
    return response.redirect_chain == [(reverse(CHANGELIST), 302)]


#: A column as text of what the database holds, so that two readings of it
#: can be compared whatever it holds. SQLite's is the hex of the value: its
#: driver converts a column by its declared type, and text that is not UTF-8
#: does not come back at all.
AS_HELD = {
    "sqlite": "hex({})",
    "postgresql": "{}::text",
    "mysql": "CAST({} AS CHAR)",
}

#: A false `enabled`, read that way.
FALSE = {"sqlite": "30", "postgresql": "false", "mysql": "0"}


def held(pk, column, using="default"):
    """What the row's column holds, or None when the row is gone."""
    conn = connections[using]
    shape = AS_HELD[conn.vendor].format(column)
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT {shape} FROM {SCHEDULES} WHERE id = %s", [pk])  # noqa: S608
        found = cursor.fetchone()
    return None if found is None else found[0]


def paused(pk, using="default"):
    return held(pk, "enabled", using) == FALSE[connections[using].vendor]


def everything_held(pk):
    return {
        field.column: held(pk, connection.ops.quote_name(field.column))
        for field in OxSchedule._meta.concrete_fields
    }


def marker():
    return OxScheduleChange.objects.get(id=1).changed_at


def not_enabled(*rows, more=0):
    """What Enable says of `rows`, and of `more` rows it counts and does not name."""
    named = ", ".join(rows) + (f" and {more} more" if more else "")
    return (
        f"Left {len(rows) + more} schedule(s) unchanged because they hold a stored "
        f"value django-ox cannot use: {named}. Open each schedule for its repair "
        "options before enabling it."
    )


def not_run(*rows):
    return (
        f"Skipped {len(rows)} schedule(s) holding a stored value django-ox cannot "
        f"use: {', '.join(rows)}. Open each schedule for its repair options."
    )


def ticks_do_not_read(*pks):
    keys = " ".join(f"--schedule-key db:{pk}" for pk in pks)
    return (
        f"Unreadable tick history may block automatic dispatch for {len(pks)} of "
        "these schedule(s). Ask an operator to stop the workers that dispatch these "
        "schedules. Use ox_prune --purge-unreadable-ticks with the target options "
        "listed below, adding --dry-run first. Check the listed rows and the tasks "
        "those ticks recorded before running without --dry-run, then start the "
        "workers again. Removing history can repeat a tick or cause re-anchoring "
        "that skips the next tick. Target options (up to 10 schedules shown; "
        f'"and N more" is not command syntax): {keys}.'
    )


def deleting_unread(*rows):
    return (
        f"{len(rows)} of the selected schedule(s) hold a stored value django-ox "
        f"cannot use: {', '.join(rows)}. Deleting them removes the rows by primary "
        "key."
    )


def not_valid(*rows):
    return (
        f"Left {len(rows)} schedule(s) disabled. Correct them before enabling "
        f"them: {', '.join(rows)}."
    )


def cannot_run(*rows):
    return (
        f"Skipped {len(rows)} schedule(s) that cannot run as written: "
        f"{', '.join(rows)}."
    )


NO_PERMISSION = (
    "Skipped 1 schedule(s) you do not have permission to change. If a task_key "
    "cannot be read or isn't registered here, only an active superuser can change "
    "its schedule."
)
RAN_ANYWAY = (
    "Ran 1 schedule(s) that were disabled or past their end time. "
    "A manual run ignores both."
)


@pytest.mark.parametrize(("vendor", "column", "literal"), UNREADABLE_ROWS)
class TestASelectionThatHoldsARowThatDoesNotRead:
    """A healthy row and one that does not read, selected together."""

    def test_disable_pauses_both(self, client, admin_user, vendor, column, literal):
        healthy = a_schedule("healthy")
        pk, column = an_unreadable_schedule("bad", vendor, column, literal)
        before = everything_held(pk)
        told = marker()
        client.force_login(admin_user)
        response = act(client, "disable_selected", [healthy.pk, pk])
        assert went_back_to_the_list(response)
        assert messages_of(response) == ["Disabled 2 schedule(s)."]
        assert paused(healthy.pk)
        assert paused(pk)
        assert marker() > told, "the workers were not told"
        after = everything_held(pk)
        if column not in ("enabled", "updated_at"):
            # The value that did not read is where it was: the pause neither
            # read it nor wrote it.
            assert after[column] == before[column]
        for name in ("name", "task_key", "cron", "every_seconds", "arguments"):
            if name != column:
                assert after[name] == before[name], name
        # The pause of the row that did not read is in the admin's log,
        # under a label that reads nothing of the row but its name.
        entry = LogEntry.objects.get()
        assert entry.action_flag == CHANGE
        assert entry.object_id == str(pk)
        assert entry.object_repr == label(pk, column)
        assert entry.get_change_message() == "Changed Enabled."

    def test_enable_leaves_it_as_it_is_and_says_which(
        self, client, admin_user, vendor, column, literal
    ):
        healthy = a_schedule("healthy", enabled=False)
        pk, column = an_unreadable_schedule(
            "bad", vendor, column, literal, enabled=False
        )
        before = everything_held(pk)
        client.force_login(admin_user)
        response = act(client, "enable_selected", [healthy.pk, pk])
        assert went_back_to_the_list(response)
        assert messages_of(response) == [
            "Enabled 1 schedule(s).",
            not_enabled(label(pk, column)),
        ]
        assert not paused(healthy.pk)
        # Nothing of it was written: not its `enabled`, where that is the
        # value that does not read, and not a new start.
        assert everything_held(pk) == before
        assert not LogEntry.objects.exists()

    def test_a_manual_run_skips_it_and_says_which(
        self, client, admin_user, vendor, column, literal
    ):
        healthy = a_schedule("healthy")
        pk, column = an_unreadable_schedule("bad", vendor, column, literal)
        before = everything_held(pk)
        client.force_login(admin_user)
        response = act(client, "run_once_now", [healthy.pk, pk])
        assert went_back_to_the_list(response)
        assert messages_of(response) == [
            "Enqueued 1 task(s).",
            not_run(label(pk, column)),
        ]
        assert OxTask.objects.count() == 1
        assert everything_held(pk) == before

    def test_delete_selected_asks_first_and_names_it_without_reading_it(
        self, client, admin_user, vendor, column, literal
    ):
        healthy = a_schedule("healthy")
        pk, column = an_unreadable_schedule("bad", vendor, column, literal)
        client.force_login(admin_user)
        response = client.post(
            reverse(CHANGELIST),
            {"action": "delete_selected", "_selected_action": [healthy.pk, pk]},
        )
        assert response.status_code == 200
        assert CONFIRMATION in [t.name for t in response.templates]
        page = body(response)
        assert ">healthy</a>" in page
        assert f">{label(pk, column)}</a>" in page
        for key in (healthy.pk, pk):
            assert f'name="_selected_action" value="{key}"' in page
        assert messages_of(response) == [deleting_unread(label(pk, column))]
        assert held(healthy.pk, "id") is not None
        assert held(pk, "id") is not None
        assert not LogEntry.objects.exists()

    def test_delete_selected_removes_both(
        self, client, admin_user, vendor, column, literal
    ):
        healthy = a_schedule("healthy")
        pk, column = an_unreadable_schedule("bad", vendor, column, literal)
        told = marker()
        client.force_login(admin_user)
        response = act(client, "delete_selected", [healthy.pk, pk], post="yes")
        assert went_back_to_the_list(response)
        assert messages_of(response) == ["Successfully deleted 2 ox schedules."]
        assert held(healthy.pk, "id") is None
        assert held(pk, "id") is None
        assert marker() > told, "the workers were not told"
        entries = LogEntry.objects.order_by("object_repr")
        assert {entry.action_flag for entry in entries} == {DELETION}
        assert {(entry.object_id, entry.object_repr) for entry in entries} == {
            (str(healthy.pk), "healthy"),
            (str(pk), label(pk, column)),
        }


@pytest.mark.parametrize(("vendor", "column", "literal"), RECOVERY_ROWS)
class TestTheRecoveryPagesDisable:
    def test_its_button_pauses_the_row(
        self, client, admin_user, vendor, column, literal
    ):
        pk, column = an_unreadable_schedule("bad", vendor, column, literal)
        client.force_login(admin_user)
        page = body(client.get(reverse(CHANGE_PAGE, args=[pk])))
        # What the page's own form submits, to where it submits it.
        form = re.search(
            r'<form method="post" action="([^"]*)">(.*?)</form>', page, re.S
        )
        assert form is not None, "the page offers no Disable"
        inputs = dict(
            re.findall(r'type="hidden" name="([^"]+)" value="([^"]*)"', form[2])
        )
        inputs.pop("csrfmiddlewaretoken", None)
        assert inputs == {
            "action": "disable_selected",
            "index": "0",
            "_selected_action": str(pk),
        }
        response = client.post(form[1], inputs, follow=True)
        assert went_back_to_the_list(response)
        assert messages_of(response) == ["Disabled 1 schedule(s)."]
        assert paused(pk)
        entry = LogEntry.objects.get()
        assert (entry.action_flag, entry.object_id) == (CHANGE, str(pk))


@pytest.mark.django_db
class TestWhoMayActOnARowThatDoesNotRead:
    def test_a_registry_permission_still_applies_to_it(self, client):
        plain = a_schedule("plain")
        pk, _ = an_unreadable_schedule("bad", task_key="restricted")
        before = everything_held(pk)
        user = staff(*EVERY_PERMISSION)
        client.force_login(user)
        for action, done in (
            ("disable_selected", "Disabled 1 schedule(s)."),
            ("enable_selected", "Enabled 1 schedule(s)."),
            ("run_once_now", "Enqueued 1 task(s)."),
        ):
            response = act(client, action, [plain.pk, pk])
            assert went_back_to_the_list(response)
            assert messages_of(response)[:2] == [done, NO_PERMISSION], action
        assert everything_held(pk) == before
        assert not LogEntry.objects.exists()
        # With the entry's own permission the row is paused like any other.
        user.user_permissions.add(Permission.objects.get(codename="view_user"))
        client.force_login(User.objects.get(pk=user.pk))
        response = act(client, "disable_selected", [pk])
        assert messages_of(response) == ["Disabled 1 schedule(s)."]
        assert paused(pk)

    def test_delete_selected_is_refused_for_it(self, client):
        plain = a_schedule("plain")
        pk, _ = an_unreadable_schedule("bad", task_key="restricted")
        client.force_login(staff(*EVERY_PERMISSION))
        selection = {"action": "delete_selected", "_selected_action": [plain.pk, pk]}
        asked = client.post(reverse(CHANGELIST), selection)
        assert asked.status_code == 200
        assert asked.context["perms_lacking"] == {"ox schedule"}
        assert 'value="yes"' not in body(asked), "the deletion can be confirmed"
        refused = client.post(reverse(CHANGELIST), {**selection, "post": "yes"})
        assert refused.status_code == 403
        assert held(plain.pk, "id") is not None
        assert held(pk, "id") is not None
        assert not LogEntry.objects.exists()

    def test_a_task_key_that_does_not_read_leaves_it_to_a_superuser(self, client):
        only_on("sqlite", ": text that is not UTF-8")
        row = a_schedule("bad")
        set_column(row.pk, "task_key", "CAST(X'72657080FF' AS TEXT)")
        before = everything_held(row.pk)
        # Every permission there is to hold short of being a superuser. Which
        # registry entry the row names is not known, so neither is the
        # permission that entry asks for.
        client.force_login(staff(*EVERY_PERMISSION, "view_user"))
        response = act(client, "disable_selected", [row.pk])
        assert messages_of(response) == ["Disabled 0 schedule(s).", NO_PERMISSION]
        refused = client.post(
            reverse(CHANGELIST),
            {"action": "delete_selected", "_selected_action": [row.pk], "post": "yes"},
        )
        assert refused.status_code == 403
        assert everything_held(row.pk) == before
        root = User.objects.create_superuser("root", "root@example.com", "pw")
        client.force_login(root)
        response = act(client, "disable_selected", [row.pk])
        assert messages_of(response) == ["Disabled 1 schedule(s)."]
        assert paused(row.pk)
        response = act(client, "delete_selected", [row.pk], post="yes")
        assert messages_of(response) == ["Successfully deleted 1 ox schedule."]
        assert held(row.pk, "id") is None

    def test_without_the_change_permission_no_action_is_run(self, client):
        pk, _ = an_unreadable_schedule()
        before = everything_held(pk)
        client.force_login(staff("view_oxschedule", "delete_oxschedule"))
        for action in ("disable_selected", "enable_selected", "run_once_now"):
            response = act(client, action, [pk])
            assert messages_of(response) == ["No action selected."], action
        assert everything_held(pk) == before
        assert not OxTask.objects.exists()


@pytest.mark.django_db
class TestTickHistoryThatDoesNotRead:
    """
    The schedule itself reads, so nothing about it is refused. A worker
    reads its newest tick before it dispatches, and that is what the person
    enabling or running it is told.
    """

    @pytest.mark.parametrize(("vendor", "literal"), UNREADABLE_NEWEST_TICKS)
    def test_every_action_goes_ahead_and_two_say_what_holds_it_back(
        self, client, admin_user, vendor, literal, caplog
    ):
        only_on(vendor)
        broken = a_schedule("broken", enabled=False)
        quiet = a_schedule("quiet", enabled=False)
        insert_tick(f"db:{broken.pk}", literal)
        both = [broken.pk, quiet.pk]
        client.force_login(admin_user)
        caplog.set_level("WARNING", logger="django_ox")

        response = act(client, "enable_selected", both)
        assert went_back_to_the_list(response)
        assert messages_of(response) == [
            "Enabled 2 schedule(s).",
            ticks_do_not_read(broken.pk),
        ]
        assert not paused(broken.pk)

        response = act(client, "run_once_now", both)
        assert messages_of(response) == [
            "Enqueued 2 task(s).",
            ticks_do_not_read(broken.pk),
        ]
        assert OxTask.objects.count() == 2

        # Pausing it and deleting it need no such word.
        response = act(client, "disable_selected", both)
        assert messages_of(response) == ["Disabled 2 schedule(s)."]
        assert paused(broken.pk)
        asked = client.post(
            reverse(CHANGELIST), {"action": "delete_selected", "_selected_action": both}
        )
        assert asked.status_code == 200
        assert messages_of(asked) == []
        response = act(client, "delete_selected", both, post="yes")
        assert messages_of(response) == ["Successfully deleted 2 ox schedules."]
        assert not OxSchedule.objects.exists()
        reported = [
            record
            for record in caplog.records
            if getattr(record, "event", None) == "schedule_tick_unreadable"
        ]
        assert {record.surface for record in reported} == {"admin"}
        assert {record.schedule_pk for record in reported} == {broken.pk}


@pytest.mark.django_db
class TestARowThatReadsAndDoesNotValidate:
    """
    Told apart from one that does not read, since what puts each right is
    different: this one is corrected in its form.
    """

    @pytest.fixture
    def three(self):
        valid = a_schedule("valid", enabled=False)
        broken = a_schedule("broken", enabled=False)
        quoted = connection.ops.quote_name("trigger")
        by_sql(
            f"UPDATE {SCHEDULES} SET {quoted} = 'cron', cron = 'banana', "  # noqa: S608
            f"every_seconds = NULL WHERE id = {broken.pk}"
        )
        pk, column = an_unreadable_schedule("bad", enabled=False)
        return valid, broken, pk, column

    def test_enable_reports_each_kind_with_its_rows(self, client, admin_user, three):
        valid, broken, pk, column = three
        client.force_login(admin_user)
        response = act(client, "enable_selected", [valid.pk, broken.pk, pk])
        assert messages_of(response) == [
            "Enabled 1 schedule(s).",
            not_valid(f"broken (pk {broken.pk})"),
            not_enabled(label(pk, column)),
        ]
        assert not paused(valid.pk)
        assert paused(broken.pk)

    def test_a_manual_run_reports_each_kind_with_its_rows(
        self, client, admin_user, three
    ):
        valid, broken, pk, column = three
        client.force_login(admin_user)
        response = act(client, "run_once_now", [valid.pk, broken.pk, pk])
        assert messages_of(response) == [
            "Enqueued 1 task(s).",
            cannot_run(f"broken (pk {broken.pk})"),
            not_run(label(pk, column)),
            RAN_ANYWAY,
        ]
        assert OxTask.objects.count() == 1


@pytest.mark.django_db
class TestManyRowsThatDoNotRead:
    def test_the_first_ten_are_named_and_the_rest_counted(self, client, admin_user):
        column, literal = ONE_PER_ENGINE[connection.vendor]
        pks = []
        for number in range(12):
            row = a_schedule(f"bad-{number:02d}", enabled=False)
            set_column(row.pk, column, literal)
            pks.append(row.pk)
        client.force_login(admin_user)
        response = act(client, "enable_selected", pks)
        first_ten = [f"bad-{n:02d} (pk {pk})" for n, pk in enumerate(pks[:10])]
        assert messages_of(response) == [
            "Enabled 0 schedule(s).",
            not_enabled(*first_ten, more=2),
        ]


def meanwhile(monkeypatch, pk, change):
    """
    Have `change` run once, after the action has read the selected rows and
    before it writes the row `pk`: what another session does to a row in
    between. The permission check on each row is where an action stands at
    that moment.
    """
    asked = OxScheduleAdmin.has_change_permission
    done = []

    def has_change_permission(self, request, obj=None):
        if obj is not None and obj.pk == pk and not done:
            done.append(True)
            change()
        return asked(self, request, obj)

    monkeypatch.setattr(OxScheduleAdmin, "has_change_permission", has_change_permission)


@pytest.mark.django_db
class TestARowThatChangesUnderTheAction:
    def test_one_that_stops_reading_is_left_alone_and_reported(
        self, client, admin_user, monkeypatch, caplog
    ):
        other = a_schedule("other", enabled=False)
        row = a_schedule("turns-bad", enabled=False)
        column, literal = ONE_PER_ENGINE[connection.vendor]
        meanwhile(monkeypatch, row.pk, lambda: set_column(row.pk, column, literal))
        client.force_login(admin_user)
        caplog.set_level("WARNING", logger="django_ox")
        response = act(client, "enable_selected", [other.pk, row.pk])
        assert went_back_to_the_list(response)
        assert messages_of(response) == [
            "Enabled 1 schedule(s).",
            not_enabled(f"turns-bad (pk {row.pk})"),
        ]
        assert not paused(other.pk)
        assert paused(row.pk)
        reported = [
            record
            for record in caplog.records
            if getattr(record, "event", None) == "schedule_admin_row_unreadable"
        ]
        assert [record.schedule_pk for record in reported] == [row.pk]
        assert reported[0].fields == [column]

    @pytest.mark.parametrize("action", ["enable_selected", "disable_selected"])
    def test_one_that_is_deleted_is_counted_as_gone(
        self, client, admin_user, monkeypatch, action
    ):
        other = a_schedule("other", enabled=action == "disable_selected")
        row = a_schedule("goes", enabled=action == "disable_selected")
        meanwhile(
            monkeypatch,
            row.pk,
            lambda: by_sql(f"DELETE FROM {SCHEDULES} WHERE id = {row.pk}"),  # noqa: S608
        )
        client.force_login(admin_user)
        response = act(client, action, [other.pk, row.pk])
        assert went_back_to_the_list(response)
        verb = "Enabled" if action == "enable_selected" else "Disabled"
        assert messages_of(response) == [
            f"{verb} 1 schedule(s).",
            "Skipped 1 schedule(s) that no longer exist.",
        ]
        assert paused(other.pk) == (action == "disable_selected")

    def test_one_the_person_may_no_longer_change_is_counted_as_such(
        self, client, monkeypatch
    ):
        # The write asks the registry entry's permission of the row as it
        # stands under its lock, which is not always the row the action read.
        other = a_schedule("other")
        row = a_schedule("retargeted")
        meanwhile(
            monkeypatch,
            row.pk,
            lambda: set_column(row.pk, "task_key", "'restricted'"),
        )
        client.force_login(staff(*EVERY_PERMISSION))
        response = act(client, "disable_selected", [other.pk, row.pk])
        assert went_back_to_the_list(response)
        assert messages_of(response) == ["Disabled 1 schedule(s).", NO_PERMISSION]
        assert paused(other.pk)
        assert not paused(row.pk)


@pytest.mark.django_db
class TestDeleteSelectedIsStillDjangos:
    def test_a_receiver_of_the_delete_signals_is_sent_every_row(
        self, client, admin_user
    ):
        # With a receiver connected, Django reads the rows it is about to
        # delete so as to hand each to the receiver, where otherwise it
        # deletes them by key in one statement. That read is the one a value
        # that does not read ends.
        healthy = a_schedule("healthy")
        pk, _ = an_unreadable_schedule()
        seen = []

        def before(sender, instance, **kwargs):
            seen.append(("before", instance.pk))

        def after(sender, instance, **kwargs):
            # The row that reads arrives whole, as Django's own delete
            # hands it over.
            name = instance.name if instance.pk == healthy.pk else None
            seen.append(("after", instance.pk, name))

        pre_delete.connect(before, sender=OxSchedule)
        post_delete.connect(after, sender=OxSchedule)
        try:
            client.force_login(admin_user)
            response = act(client, "delete_selected", [healthy.pk, pk], post="yes")
        finally:
            pre_delete.disconnect(before, sender=OxSchedule)
            post_delete.disconnect(after, sender=OxSchedule)
        assert messages_of(response) == ["Successfully deleted 2 ox schedules."]
        assert set(seen) == {
            ("before", healthy.pk),
            ("before", pk),
            ("after", healthy.pk, "healthy"),
            ("after", pk, None),
        }
        assert held(healthy.pk, "id") is None
        assert held(pk, "id") is None

    def test_it_is_not_offered_where_the_site_has_switched_it_off(
        self, client, admin_user, monkeypatch
    ):
        row = a_schedule("kept")
        # site is a lazy proxy, and _actions lives only on the AdminSite
        # behind it. pytest 9.2.0 restores an attribute patched through
        # such a proxy by deleting it (pytest-dev/pytest#15099), so the
        # patch goes on the AdminSite itself.
        monkeypatch.setattr(site._wrapped, "_actions", {})
        client.force_login(admin_user)
        listed = client.get(reverse(CHANGELIST))
        assert 'value="delete_selected"' not in body(listed)
        assert 'value="disable_selected"' in body(listed)
        response = act(client, "delete_selected", [row.pk], post="yes")
        assert messages_of(response) == ["No action selected."]
        assert held(row.pk, "id") is not None

    def test_a_selection_that_is_gone_deletes_nothing_and_says_nothing(
        self, client, admin_user
    ):
        kept = a_schedule("kept")
        client.force_login(admin_user)
        response = act(client, "delete_selected", [987654], post="yes")
        assert went_back_to_the_list(response)
        assert messages_of(response) == []
        assert held(kept.pk, "id") is not None
        assert not LogEntry.objects.exists()

    def test_the_signals_carry_the_selection_as_where_the_deletion_came_from(
        self, client, admin_user
    ):
        # As Django's own "Delete selected" has them: each row an instance,
        # and the queryset the action was handed as the deletion's origin.
        rows = [a_schedule("first"), a_schedule("second")]
        origins = []

        def after(sender, instance, origin, **kwargs):
            origins.append(origin)

        post_delete.connect(after, sender=OxSchedule)
        try:
            client.force_login(admin_user)
            act(client, "delete_selected", [row.pk for row in rows], post="yes")
        finally:
            post_delete.disconnect(after, sender=OxSchedule)
        assert len(origins) == 2
        assert origins[0] is origins[1]
        assert isinstance(origins[0], QuerySet)
        assert origins[0].model is OxSchedule

    def test_it_costs_the_same_whatever_the_selection(self, client, admin_user):
        client.force_login(admin_user)

        def deleting(count):
            pks = [a_schedule(f"s{count:03d}-{n:03d}").pk for n in range(count)]
            with CaptureQueriesContext(connection) as queries:
                response = client.post(
                    reverse(CHANGELIST),
                    {
                        "action": "delete_selected",
                        "_selected_action": [str(pk) for pk in pks],
                        "post": "yes",
                    },
                )
            assert response.status_code == 302
            assert not OxSchedule.objects.filter(pk__in=pks).exists()
            return [q["sql"].lstrip() for q in queries.captured_queries]

        def kinds(statements):
            said = []
            for sql in statements:
                table = (
                    "marker"
                    if f"{SCHEDULES}change" in sql
                    else "schedules"
                    if SCHEDULES in sql
                    else ""
                )
                said.append(f"{sql.split()[0].upper()} {table}".strip())
            return said

        # Once before any is counted, for what Django reads once a process
        # and then remembers.
        deleting(2)
        three, thirty, hundred = (deleting(count) for count in (3, 30, 100))

        # The whole request, the page and the admin's log with it: the same
        # statements for three rows, for thirty and for a page of a hundred.
        row_locks = connection.features.has_select_for_update
        assert len(three) == (22 if row_locks else 23)
        assert kinds(thirty) == kinds(three)
        assert kinds(hundred) == kinds(three)
        # What the deletion itself sends, once the log is written: the
        # selection read by key and then by row, the rows locked and read
        # for the permission by one statement, one delete, and the workers
        # told once, in the two blocks Django opens for them.
        logged = next(i for i, sql in enumerate(three) if "django_admin_log" in sql)
        the_deletion = [
            "SELECT schedules",
            "SELECT schedules",
            "SAVEPOINT",
            "SELECT schedules",
            "DELETE schedules",
            "SAVEPOINT",
            "SELECT marker",
            "UPDATE marker",
            "RELEASE",
            "RELEASE",
            "RELEASE",
        ]
        if not row_locks:
            # The no-op write that makes the transaction the writer.
            the_deletion.insert(3, "UPDATE schedules")
        assert kinds(three)[logged + 1 :] == the_deletion
        locking = [
            sql
            for sql, kind in zip(three, kinds(three), strict=True)
            if kind == "SELECT schedules" and "FOR UPDATE" in sql
        ]
        assert len(locking) == (1 if row_locks else 0)


@pytest.mark.django_db(databases=["default", "alt"])
class TestTheActionsReadTheDatabaseTheSchedulesAreWrittenTo:
    """Under a router that sends reads to a replica, as `_selected` says."""

    class _ReplicaReads:
        def db_for_read(self, model, **hints):
            return "default" if model._meta.app_label == "django_ox" else None

        def db_for_write(self, model, **hints):
            return "alt" if model._meta.app_label == "django_ox" else None

        def allow_relation(self, obj1, obj2, **hints):
            return True

        def allow_migrate(self, db, app_label, **hints):
            return None

    def test_a_mixed_selection_is_acted_on_where_it_is_written(
        self, client, admin_user, settings
    ):
        settings.DATABASE_ROUTERS = [self._ReplicaReads()]
        healthy = a_schedule("healthy")
        bad = a_schedule("bad")
        assert healthy._state.db == "alt"
        # alt is SQLite whatever the default database is.
        with connections["alt"].cursor() as cursor:
            cursor.execute(
                f"UPDATE {SCHEDULES} SET end_time = '2026-02-30 00:00:00' "  # noqa: S608
                f"WHERE id = {int(bad.pk)}"
            )
        both = [healthy.pk, bad.pk]
        client.force_login(admin_user)
        response = act(client, "disable_selected", both)
        assert messages_of(response) == ["Disabled 2 schedule(s)."]
        assert paused(healthy.pk, "alt")
        assert paused(bad.pk, "alt")
        response = act(client, "run_once_now", both)
        assert messages_of(response)[:2] == [
            "Enqueued 1 task(s).",
            not_run(f"bad (pk {bad.pk})"),
        ]
        assert OxTask.objects.using("alt").count() == 1
        response = act(client, "delete_selected", both, post="yes")
        assert messages_of(response) == ["Successfully deleted 2 ox schedules."]
        assert held(healthy.pk, "id", "alt") is None
        assert held(bad.pk, "id", "alt") is None


@pytest.mark.django_db(transaction=True)
class TestOutsideATransaction:
    """
    An action runs outside a transaction unless the project sets
    ATOMIC_REQUESTS, so each row's write is a transaction of its own. Every
    other test here runs inside the one its test is wrapped in.
    """

    def test_each_action_on_a_mixed_selection(self, client, admin_user):
        healthy = a_schedule("healthy")
        pk, column = an_unreadable_schedule()
        both = [healthy.pk, pk]
        assert connection.get_autocommit()
        client.force_login(admin_user)
        response = act(client, "disable_selected", both)
        assert messages_of(response) == ["Disabled 2 schedule(s)."]
        assert paused(healthy.pk)
        assert paused(pk)
        response = act(client, "enable_selected", both)
        assert messages_of(response) == [
            "Enabled 1 schedule(s).",
            not_enabled(label(pk, column)),
        ]
        response = act(client, "run_once_now", both)
        assert messages_of(response)[:2] == [
            "Enqueued 1 task(s).",
            not_run(label(pk, column)),
        ]
        response = act(client, "delete_selected", both, post="yes")
        assert messages_of(response) == ["Successfully deleted 2 ox schedules."]
        assert held(healthy.pk, "id") is None
        assert held(pk, "id") is None
        # Nothing above left a transaction open behind it.
        assert connection.get_autocommit()
        assert not connection.in_atomic_block
        assert client.get(reverse(CHANGELIST)).status_code == 200
