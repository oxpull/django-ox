"""The schedule admin: the first place a person authors a schedule row."""

from datetime import timedelta

import pytest
from django.contrib.auth.models import Permission, User
from django.core.exceptions import PermissionDenied
from django.urls import reverse
from django.utils import timezone

from django_ox.models import OxSchedule, OxScheduleTick, OxTask
from django_ox.registry import ScheduleKind, register
from django_ox.schedules import STORED_KEY_PREFIX, schedule_source_from_options
from django_ox.stored import create_schedule, update_schedule

from . import sources, tasks

pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.add))
    register(
        ScheduleKind(key="restricted", task=tasks.add, permission="auth.view_user")
    )


@pytest.fixture
def admin_user(db):
    return User.objects.create_superuser("root", "root@example.com", "pw")


@pytest.fixture
def staff_user(db):
    user = User.objects.create_user("staff", "staff@example.com", "pw", is_staff=True)
    for codename in (
        "add_oxschedule",
        "change_oxschedule",
        "delete_oxschedule",
        "view_oxschedule",
    ):
        user.user_permissions.add(Permission.objects.get(codename=codename))
    return user


def a_schedule(**over):
    fields = {
        "name": "nightly",
        "task_key": "report",
        "trigger": "cron",
        "cron": "0 2 * * *",
        "start_time": timezone.now() - timedelta(days=1),
    }
    fields.update(over)
    return create_schedule(**fields)


def _form_datetime(at):
    """
    Split an instant the way the admin's two-part datetime widget reads it
    back: in the project's timezone.

    Formatting an aware value straight off a row splits its UTC wall clock
    instead, so the form reads back an instant a whole UTC offset from the
    one the row holds. That passes only where the project's zone is west of
    UTC by more than the margin the test left itself, and the smallest of
    those margins is an hour. America/Chicago is west by five, so these
    tests hold under the settings this suite runs; under any zone an hour
    or more east of UTC they would fail every time.
    """
    local = timezone.localtime(at) if timezone.is_aware(at) else at
    return local.strftime("%Y-%m-%d"), local.strftime("%H:%M:%S")


ADD_URL = "admin:django_ox_oxschedule_add"
CHANGE_URL = "admin:django_ox_oxschedule_change"


class TestTheRegistryIsEnforcedOnThePostNotJustTheWidget:
    def test_a_post_naming_an_unregistered_task_is_rejected(self, client, admin_user):
        # A ChoiceField validates membership server-side, so this is refused
        # by the form rather than only missing from the rendered select. The
        # model's own check covers the paths that build no form at all, which
        # tests/test_stored_schedules.py exercises.
        client.force_login(admin_user)
        response = client.post(
            reverse(ADD_URL),
            {
                "name": "evil",
                "task_key": "os.system",
                "trigger": "cron",
                "cron": "0 2 * * *",
                "arguments": "{}",
                "phase_seconds": 0,
            },
        )
        assert response.status_code == 200  # redisplayed with errors
        assert not OxSchedule.objects.filter(name="evil").exists()

    def test_a_valid_post_creates_a_schedule_with_a_boundary(self, client, admin_user):
        client.force_login(admin_user)
        before = timezone.now()
        response = client.post(
            reverse(ADD_URL),
            {
                "name": "nightly",
                "task_key": "report",
                "trigger": "cron",
                "cron": "0 2 * * *",
                "arguments": "{}",
                "phase_seconds": 0,
                "enabled": "on",
            },
        )
        assert response.status_code == 302, response.context["errors"]
        row = OxSchedule.objects.get(name="nightly")
        # The service layer ran, so the boundary was written by the creator
        # rather than left for a worker to discover.
        assert row.start_time >= before

    def test_the_task_field_offers_only_registered_keys(self, client, admin_user):
        client.force_login(admin_user)
        response = client.get(reverse(ADD_URL))
        field = response.context["adminform"].form.fields["task_key"]
        assert [key for key, _ in field.choices] == ["report", "restricted"]


class TestTheAdminGoesThroughTheServiceLayer:
    def test_retiming_through_the_admin_moves_the_boundary(self, client, admin_user):
        row = a_schedule()
        original = row.start_time
        client.force_login(admin_user)
        response = client.post(
            reverse(CHANGE_URL, args=[row.pk]),
            {
                "name": row.name,
                "task_key": row.task_key,
                "trigger": "cron",
                "cron": "0 3 * * *",
                "arguments": "{}",
                "phase_seconds": 0,
                "enabled": "on",
            },
        )
        assert response.status_code == 302
        row.refresh_from_db()
        assert row.cron == "0 3 * * *"
        assert row.start_time > original, (
            "a retimed schedule must not keep a boundary set for its old timing"
        )


class TestPerEntryPermission:
    def test_the_service_layer_refuses_without_the_permission(self, staff_user):
        with pytest.raises(PermissionDenied):
            create_schedule(
                name="r",
                task_key="restricted",
                trigger="cron",
                cron="0 2 * * *",
                user=staff_user,
            )

    def test_the_service_layer_allows_with_the_permission(self, staff_user):
        staff_user.user_permissions.add(Permission.objects.get(codename="view_user"))
        staff_user = User.objects.get(pk=staff_user.pk)  # drop the perm cache
        row = create_schedule(
            name="r",
            task_key="restricted",
            trigger="cron",
            cron="0 2 * * *",
            user=staff_user,
        )
        assert row.pk

    def test_an_unrestricted_entry_needs_no_extra_permission(self, staff_user):
        assert create_schedule(
            name="ok",
            task_key="report",
            trigger="cron",
            cron="0 2 * * *",
            user=staff_user,
        ).pk

    def test_the_admin_will_not_save_a_restricted_row_without_the_permission(
        self, client, staff_user
    ):
        # With view permission but not change, Django renders the form
        # read-only rather than refusing outright, so a 200 proves nothing.
        # What matters is whether a POST can write.
        row = a_schedule(name="r", task_key="restricted")
        client.force_login(staff_user)
        client.post(
            reverse(CHANGE_URL, args=[row.pk]),
            {
                "name": row.name,
                "task_key": "restricted",
                "trigger": "cron",
                "cron": "0 4 * * *",
                "arguments": "{}",
                "phase_seconds": 0,
                "enabled": "on",
            },
        )
        row.refresh_from_db()
        assert row.cron == "0 2 * * *", "the restricted row must be unchanged"

    def test_the_admin_reports_the_row_as_unchangeable(self, client, staff_user):
        row = a_schedule(name="r", task_key="restricted")
        client.force_login(staff_user)
        from django.contrib.admin.sites import site

        from django_ox.admin import OxScheduleAdmin

        model_admin = site._registry[OxSchedule]
        assert isinstance(model_admin, OxScheduleAdmin)
        request = client.request().wsgi_request
        request.user = staff_user
        assert not model_admin.has_change_permission(request, row)
        assert model_admin.has_change_permission(request, a_schedule(name="plain"))


class TestRunOnceNowMatchesADispatchedTick:
    def test_it_passes_the_cleaned_arguments(self, client, admin_user):
        # A dispatched tick carries what the form cleans to, so running one
        # from the admin must build it the same way: the row's raw values
        # would make the same schedule differ by how it was started.
        from django import forms

        from django_ox.registry import ArgsForm, ScheduleKind, register

        class _Args(ArgsForm):
            count = forms.IntegerField()

        register(ScheduleKind(key="counted", task=tasks.add, form=_Args))
        row = a_schedule(name="counted", task_key="counted", arguments={"count": "5"})
        client.force_login(admin_user)
        client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {"action": "run_once_now", "_selected_action": [str(row.pk)]},
            follow=True,
        )
        assert OxTask.objects.count() == 1
        assert OxTask.objects.get().kwargs == {"count": 5}, (
            "a manual run carried the raw string where a tick carries the int"
        )

    def test_a_row_that_cannot_run_is_reported_not_raised(self, client, admin_user):
        row = OxSchedule.objects.create(
            name="broken",
            task_key="report",
            trigger="cron",
            cron="banana",
            start_time=timezone.now(),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        client.force_login(admin_user)
        response = client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {"action": "run_once_now", "_selected_action": [str(row.pk)]},
            follow=True,
        )
        assert response.status_code == 200
        assert OxTask.objects.count() == 0


class TestActions:
    def _post_action(self, client, action, pks):
        return client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {"action": action, "_selected_action": [str(pk) for pk in pks]},
            follow=True,
        )

    def test_disable_then_enable(self, client, admin_user):
        row = a_schedule()
        client.force_login(admin_user)
        self._post_action(client, "disable_selected", [row.pk])
        row.refresh_from_db()
        assert not row.enabled
        paused_boundary = row.start_time
        self._post_action(client, "enable_selected", [row.pk])
        row.refresh_from_db()
        assert row.enabled
        assert row.start_time > paused_boundary, (
            "re-enabling must move the boundary, or a pause accumulates a "
            "backlog that fires all at once"
        )

    def test_run_once_now_enqueues_without_consuming_a_tick(self, client, admin_user):
        from django_ox.models import OxScheduleTick

        row = a_schedule()
        client.force_login(admin_user)
        self._post_action(client, "run_once_now", [row.pk])
        assert OxTask.objects.count() == 1
        assert not OxScheduleTick.objects.exists(), (
            "a manual run is not a tick and must not suppress the scheduled one"
        )

    def test_an_action_skips_rows_the_user_may_not_change(self, client, staff_user):
        allowed = a_schedule(name="allowed")
        restricted = a_schedule(name="restricted-row", task_key="restricted")
        client.force_login(staff_user)
        self._post_action(client, "disable_selected", [allowed.pk, restricted.pk])
        allowed.refresh_from_db()
        restricted.refresh_from_db()
        assert not allowed.enabled
        assert restricted.enabled, "the restricted row must be left alone"


class TestRunOnceNowSaysWhatItRan:
    """
    A manual run ignores both bounds a schedule carries: it fires a row
    that is disabled and a row that is past its end time. That is the
    point of it -- it is the one way to run a paused schedule -- but the
    changelist shows paused and running rows together, an admin action has
    no confirmation step, and the only report was a success count. An
    operator who mis-selected a paused production schedule had nothing
    telling them it had run.
    """

    def _run(self, client, pks):
        return client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {
                "action": "run_once_now",
                "_selected_action": [str(pk) for pk in pks],
            },
            follow=True,
        )

    def test_a_paused_schedule_runs_and_the_report_says_so(self, client, admin_user):
        row = a_schedule()
        client.force_login(admin_user)
        self._run(client, [row.pk])  # disable through the documented action
        client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {"action": "disable_selected", "_selected_action": [str(row.pk)]},
            follow=True,
        )
        row.refresh_from_db()
        assert not row.enabled
        response = self._run(client, [row.pk])
        messages = [str(m) for m in response.context["messages"]]
        assert "Enqueued 1 task(s)." in messages
        assert (
            "Ran 1 schedule(s) that were disabled or past their end time. "
            "A manual run ignores both." in messages
        )

    def test_a_schedule_past_its_end_time_runs_and_the_report_says_so(
        self, client, admin_user
    ):
        row = a_schedule(end_time=timezone.now() - timedelta(hours=1))
        client.force_login(admin_user)
        response = self._run(client, [row.pk])
        messages = [str(m) for m in response.context["messages"]]
        assert "Enqueued 1 task(s)." in messages
        assert any("past their end time" in m for m in messages)

    def test_a_live_schedule_draws_no_such_report(self, client, admin_user):
        row = a_schedule(end_time=timezone.now() + timedelta(days=1))
        client.force_login(admin_user)
        response = self._run(client, [row.pk])
        messages = [str(m) for m in response.context["messages"]]
        assert "Enqueued 1 task(s)." in messages
        assert not any("past their end time" in m for m in messages), (
            "an ordinary run was reported as an override"
        )

    def test_only_the_rows_that_actually_ran_are_counted(self, client, admin_user):
        # A paused row that cannot be built is skipped, not run, so counting
        # it as an override would name a run that never happened.
        broken = OxSchedule.objects.create(
            name="broken",
            task_key="report",
            trigger="cron",
            cron="banana",
            enabled=False,
            start_time=timezone.now(),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        client.force_login(admin_user)
        response = self._run(client, [broken.pk])
        messages = [str(m) for m in response.context["messages"]]
        assert "Enqueued 0 task(s)." in messages
        assert "Skipped 1 schedule(s) that cannot run as written." in messages
        assert not any("ignores both" in m for m in messages)

    def test_the_end_time_is_on_the_changelist_where_rows_are_selected(
        self, client, admin_user
    ):
        a_schedule(end_time=timezone.now() - timedelta(hours=1))
        client.force_login(admin_user)
        body = client.get(
            reverse("admin:django_ox_oxschedule_changelist")
        ).content.decode()
        assert "End time" in body, (
            "expiry is invisible at the moment the selection is made"
        )


class TestRunOnceNowReportsAPermissionRefusal:
    """
    A registry entry may declare its own permission, and a user without it
    cannot run that schedule. The refusal is enforced; what was missing was
    the report. The action counted nothing and said "Enqueued 0 task(s)."
    in the success style, so an operator refused a payroll schedule could
    not tell a permission refusal from a broken row, an unregistered task
    or a misconfigured backend -- while Disable, on the same row, said so
    plainly.
    """

    def _post(self, client, action, pks):
        return client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {"action": action, "_selected_action": [str(pk) for pk in pks]},
            follow=True,
        )

    def test_a_refused_row_is_reported_rather_than_counted_as_nothing(
        self, client, staff_user
    ):
        row = a_schedule(name="payroll", task_key="restricted")
        client.force_login(staff_user)
        response = self._post(client, "run_once_now", [row.pk])
        messages = [str(m) for m in response.context["messages"]]
        assert "Enqueued 0 task(s)." in messages
        assert "Skipped 1 schedule(s) you do not have permission to change." in messages
        assert OxTask.objects.count() == 0

    def test_the_wording_is_the_one_the_sibling_action_already_uses(
        self, client, staff_user
    ):
        # Same row, same user, both actions: an operator who has seen one
        # report reads the other without learning a second vocabulary.
        row = a_schedule(name="payroll", task_key="restricted")
        client.force_login(staff_user)
        wordings = {}
        for action in ("disable_selected", "run_once_now"):
            response = self._post(client, action, [row.pk])
            wordings[action] = sorted(
                str(m) for m in response.context["messages"] if "permission" in str(m)
            )
        assert wordings["run_once_now"], "the run action reported no refusal at all"
        assert wordings["run_once_now"] == wordings["disable_selected"], (
            f"two wordings for one refusal: {wordings}"
        )

    def test_a_partial_refusal_is_not_reported_as_plain_success(
        self, client, staff_user
    ):
        allowed = a_schedule(name="allowed")
        refused = a_schedule(name="payroll", task_key="restricted")
        client.force_login(staff_user)
        response = self._post(client, "run_once_now", [allowed.pk, refused.pk])
        messages = [str(m) for m in response.context["messages"]]
        assert "Enqueued 1 task(s)." in messages
        assert "Skipped 1 schedule(s) you do not have permission to change." in messages
        assert OxTask.objects.count() == 1

    def test_a_broken_row_is_still_reported_as_broken(self, client, admin_user):
        # The other branch keeps its own wording: a superuser is refused
        # nothing, and telling them a row is unrunnable is the true report.
        OxSchedule.objects.create(
            name="broken",
            task_key="report",
            trigger="cron",
            cron="banana",
            start_time=timezone.now(),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        client.force_login(admin_user)
        response = self._post(
            client, "run_once_now", [OxSchedule.objects.get(name="broken").pk]
        )
        messages = [str(m) for m in response.context["messages"]]
        assert "Skipped 1 schedule(s) that cannot run as written." in messages
        assert not any("permission" in m for m in messages)


class TestTheDispatchingBackendIsFoundByClassNotByName:
    """
    A project may point SCHEDULE_SOURCE at its own subclass of
    DatabaseScheduleSource -- to override how a row becomes a schedule,
    say. The loader the worker uses accepts any class with a schedules()
    method, so a subclass under any name is a supported configuration.

    The admin has to find the same backend the worker dispatches from,
    because "Run selected schedules once now" enqueues through it. Matched
    by name, a subclass called anything else was missed: the action fell
    back to the default alias, and depending on the project either
    enqueued to the wrong backend while reporting success, or reported a
    healthy schedule as one that cannot run.
    """

    def _run(self, client, pk):
        return client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {"action": "run_once_now", "_selected_action": [str(pk)]},
            follow=True,
        )

    def _two_backends(self, settings, source_path):
        # Both serve the same queue, so the enqueue succeeds either way and
        # the row's backend_name is what says which one the action chose.
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            },
            "sched": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {"SCHEDULE_SOURCE": source_path},
            },
        }

    def test_a_subclass_under_another_name_is_the_dispatching_backend(
        self, client, admin_user, settings
    ):
        self._two_backends(settings, "tests.sources.RowSource")
        row = a_schedule()
        client.force_login(admin_user)
        self._run(client, row.pk)
        assert OxTask.objects.get().backend_name == "sched", (
            "the manual run went to a backend no worker dispatches from"
        )

    def test_the_configured_class_is_the_one_that_builds_the_schedule(
        self, client, admin_user, settings
    ):
        # The subclass decides what a row becomes. Building with the base
        # class would quietly run something the project did not define.
        self._two_backends(settings, "tests.sources.CountingSource")
        from tests import sources

        sources.CountingSource.built = 0
        row = a_schedule()
        client.force_login(admin_user)
        self._run(client, row.pk)
        assert sources.CountingSource.built == 1, (
            "the action built the schedule with the base class"
        )

    def test_the_shipped_path_still_resolves(self, client, admin_user, settings):
        self._two_backends(settings, "django_ox.stored.DatabaseScheduleSource")
        row = a_schedule()
        client.force_login(admin_user)
        self._run(client, row.pk)
        assert OxTask.objects.get().backend_name == "sched"

    @pytest.mark.parametrize(
        "path",
        [
            "tests.sources.NotASource",
            "tests.sources.NoSuchNameAtAll",
            "django_ox.stored.DatabaseScheduleSource.nope",
        ],
    )
    def test_something_that_is_not_one_is_not_taken_for_one(
        self, client, admin_user, settings, path
    ):
        # A name that does not import, or imports to something that is not
        # a schedule source, must not be read as the dispatching backend.
        self._two_backends(settings, path)
        row = a_schedule()
        client.force_login(admin_user)
        self._run(client, row.pk)
        assert OxTask.objects.get().backend_name == "default"


class TestARefusedTaskKeyIsAFieldErrorAndNotA403:
    """
    A registry entry may declare its own permission. The write functions
    enforce it on every path and that is where the guarantee lives, so
    nothing unpermitted has ever been written. What the add form did was
    offer the key, take the whole submission, and answer a bare 403: no
    admin chrome, no link back, no field error, and every value the person
    typed discarded. The same form answers any other validation failure
    with the page and the values intact.

    The dropdown deliberately still offers every registered key. Narrowing
    it per user would make the change form for an existing restricted row
    fail on its own key, and it hides which keys exist rather than saying
    what is needed.
    """

    def _add(self, client, **over):
        data = {
            "name": "payroll-run",
            "task_key": "restricted",
            "trigger": "cron",
            "cron": "0 2 * * *",
            "arguments": "{}",
            "phase_seconds": 0,
            "enabled": "on",
        }
        data.update(over)
        return client.post(reverse(ADD_URL), data), data

    def test_the_refusal_comes_back_as_a_field_error(self, client, staff_user):
        client.force_login(staff_user)
        response, _ = self._add(client)
        assert response.status_code == 200, "a bare 403 discarded the submission"
        errors = response.context["adminform"].form.errors
        assert "task_key" in errors
        assert "auth.view_user" in str(errors["task_key"]), (
            "the error does not name the permission that would allow it"
        )
        assert not OxSchedule.objects.filter(name="payroll-run").exists()

    def test_what_the_person_typed_is_still_there(self, client, staff_user):
        client.force_login(staff_user)
        response, _ = self._add(client)
        body = response.content.decode()
        for value in ("payroll-run", "0 2 * * *"):
            assert value in body, f"{value!r} was discarded by the refusal"

    def test_the_same_submission_with_the_permission_is_written(
        self, client, staff_user
    ):
        staff_user.user_permissions.add(Permission.objects.get(codename="view_user"))
        client.force_login(User.objects.get(pk=staff_user.pk))
        response, _ = self._add(client)
        assert response.status_code == 302
        assert OxSchedule.objects.get(name="payroll-run").task_key == "restricted"

    def test_retargeting_an_allowed_row_at_a_refused_key_is_a_field_error(
        self, client, staff_user
    ):
        row = a_schedule(name="plain")
        client.force_login(staff_user)
        response = client.post(
            reverse(CHANGE_URL, args=[row.pk]),
            {
                "name": "plain",
                "task_key": "restricted",
                "trigger": "cron",
                "cron": "0 2 * * *",
                "arguments": "{}",
                "phase_seconds": 0,
                "enabled": "on",
            },
        )
        assert response.status_code == 200
        assert "task_key" in response.context["adminform"].form.errors
        row.refresh_from_db()
        assert row.task_key == "report", "the row was retargeted anyway"

    def test_the_dropdown_still_offers_every_registered_key(self, client, staff_user):
        # Not a filter: a narrowed dropdown would make the change form for
        # an existing restricted row fail on the key the row already holds.
        client.force_login(staff_user)
        response = client.get(reverse(ADD_URL))
        field = response.context["adminform"].form.fields["task_key"]
        assert [key for key, _ in field.choices] == ["report", "restricted"]

    def test_an_unrestricted_key_is_untouched(self, client, staff_user):
        client.force_login(staff_user)
        response, _ = self._add(client, task_key="report")
        assert response.status_code == 302
        assert OxSchedule.objects.get(name="payroll-run").task_key == "report"

    def test_the_write_function_still_refuses_without_a_form(self, staff_user):
        # The guarantee is in django_ox.stored and stays there: the form
        # only puts the refusal in front of the person.
        with pytest.raises(PermissionDenied):
            create_schedule(
                name="no-form",
                task_key="restricted",
                trigger="cron",
                cron="0 2 * * *",
                user=staff_user,
            )


class TestTheAdminSaysWhenNothingWillDispatchWhatItWrites:
    """
    The admin registers whenever django.contrib.admin is installed, and
    the rows it writes are dispatched only by a worker whose backend names
    a DatabaseScheduleSource in OPTIONS["SCHEDULE_SOURCE"]. A project that
    exposes tasks with @schedulable and never sets that option gets a
    working-looking admin whose schedules do nothing: no error, no log, no
    system check, "never" in the Last tick column, and a manual run that
    genuinely enqueues, which is positive evidence for a belief that is
    false.

    A system check cannot cover it -- a check runs without importing
    application code, so it can see SCHEDULABLE_TASKS in settings but
    never an @schedulable decorator -- and it needs a database read to
    know whether any row exists. The admin is where the registry is
    populated and where the person is standing when they make the
    mistake.
    """

    UNSET = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {},
        }
    }
    SET = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"},
        }
    }

    def _messages(self, response):
        return [str(m) for m in response.context["messages"]]

    def _warned(self, response):
        return any("never dispatched" in m for m in self._messages(response))

    def test_the_changelist_says_so(self, client, admin_user, settings):
        settings.TASKS = self.UNSET
        client.force_login(admin_user)
        response = client.get(reverse("admin:django_ox_oxschedule_changelist"))
        assert self._warned(response), (
            "a schedule saved here would never run and the page said nothing"
        )

    def test_the_add_page_says_so_before_the_row_is_written(
        self, client, admin_user, settings
    ):
        settings.TASKS = self.UNSET
        client.force_login(admin_user)
        assert self._warned(client.get(reverse(ADD_URL)))

    def test_a_manual_run_no_longer_teaches_the_wrong_lesson(
        self, client, admin_user, settings
    ):
        # The one action that works without a source. Its success message
        # is the strongest evidence the user has that the wiring is right.
        settings.TASKS = self.UNSET
        row = a_schedule()
        client.force_login(admin_user)
        response = client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {"action": "run_once_now", "_selected_action": [str(row.pk)]},
            follow=True,
        )
        messages = self._messages(response)
        assert "Enqueued 1 task(s)." in messages
        assert self._warned(response), "the run reported success and nothing else"

    def test_a_configured_project_is_not_warned(self, client, admin_user, settings):
        settings.TASKS = self.SET
        client.force_login(admin_user)
        assert not self._warned(client.get(reverse(ADD_URL)))
        assert not self._warned(
            client.get(reverse("admin:django_ox_oxschedule_changelist"))
        )

    def test_a_projects_own_source_is_not_warned_about(
        self, client, admin_user, settings
    ):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {"SCHEDULE_SOURCE": "tests.sources.RowSource"},
            }
        }
        client.force_login(admin_user)
        assert not self._warned(
            client.get(reverse("admin:django_ox_oxschedule_changelist"))
        )

    def test_the_warning_is_not_repeated_by_the_action_post(
        self, client, admin_user, settings
    ):
        settings.TASKS = self.UNSET
        row = a_schedule()
        client.force_login(admin_user)
        response = client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {"action": "run_once_now", "_selected_action": [str(row.pk)]},
            follow=True,
        )
        said = [m for m in self._messages(response) if "never dispatched" in m]
        assert len(said) == 1, f"the same warning was shown twice: {said}"


class TestASourceTheWorkerAcceptsIsNotReportedAsUndispatched:
    """
    The worker's loader builds the class SCHEDULE_SOURCE names and asks
    whether it answers schedules(). A source written by composition
    rather than by subclassing passes that test, and its rows dispatch.
    Tested here against issubclass instead, such a project was told on
    the page that its schedules are stored and never dispatched, and its
    manual run went to a backend that dispatches nothing.
    """

    def _tasks(self, path):
        # Both backends serve the same queue, so the enqueue succeeds
        # either way and the row's backend_name says which was chosen.
        return {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            },
            "sched": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {"SCHEDULE_SOURCE": path},
            },
        }

    def _messages(self, response):
        return [str(m) for m in response.context["messages"]]

    def _warned(self, response):
        return any("never dispatched" in m for m in self._messages(response))

    def _run(self, client, pk):
        return client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {"action": "run_once_now", "_selected_action": [str(pk)]},
            follow=True,
        )

    def test_the_worker_reads_the_row_through_it(self, settings):
        # The claim the admin warning contradicts, made through the
        # loader the worker itself calls.
        settings.TASKS = self._tasks("tests.sources.DuckSource")
        a_schedule()
        options = settings.TASKS["sched"]["OPTIONS"]
        source = schedule_source_from_options(options, "sched")
        assert isinstance(source, sources.DuckSource)
        assert len(source.schedules()) == 1

    def test_the_changelist_does_not_call_it_undispatched(
        self, client, admin_user, settings
    ):
        settings.TASKS = self._tasks("tests.sources.DuckSource")
        client.force_login(admin_user)
        assert not self._warned(
            client.get(reverse("admin:django_ox_oxschedule_changelist"))
        )

    def test_the_add_page_does_not_call_it_undispatched(
        self, client, admin_user, settings
    ):
        settings.TASKS = self._tasks("tests.sources.DuckSource")
        client.force_login(admin_user)
        assert not self._warned(client.get(reverse(ADD_URL)))

    def test_a_manual_run_goes_to_the_backend_that_dispatches_it(
        self, client, admin_user, settings
    ):
        settings.TASKS = self._tasks("tests.sources.DuckSource")
        row = a_schedule()
        client.force_login(admin_user)
        self._run(client, row.pk)
        assert OxTask.objects.get().backend_name == "sched"

    def test_a_class_that_answers_nothing_is_still_not_a_source(
        self, client, admin_user, settings
    ):
        # The loader refuses a class it can build that has no
        # schedules(), and so must this.
        settings.TASKS = self._tasks("tests.sources.NoSchedulesMethod")
        row = a_schedule()
        client.force_login(admin_user)
        response = self._run(client, row.pk)
        assert OxTask.objects.get().backend_name == "default"
        assert self._warned(response)

    def test_a_class_that_cannot_be_built_is_still_not_a_source(
        self, client, admin_user, settings
    ):
        # tests.sources.NotASource takes no arguments. The loader fails on
        # it; the page has to keep loading and treat it as not the one.
        settings.TASKS = self._tasks("tests.sources.NotASource")
        row = a_schedule()
        client.force_login(admin_user)
        response = self._run(client, row.pk)
        assert OxTask.objects.get().backend_name == "default"
        assert self._warned(response)


class TestTheEmptyRegistryHelpTextSaysWhereToPutTheDecorator:
    def test_it_names_the_module_that_is_imported_and_the_setting(
        self, client, admin_user, monkeypatch
    ):
        # @schedulable registers nothing unless its module is imported, and
        # the only module django-ox imports for you is each installed app's
        # `tasks`. A person who put the decorator in myapp/jobs.py was told
        # to register one with @schedulable, which is what they had done.
        monkeypatch.setattr("django_ox.registry._registry", {})
        monkeypatch.setattr("django_ox.registry._discovered", True)
        client.force_login(admin_user)
        response = client.get(reverse(ADD_URL))
        help_text = response.context["adminform"].form.fields["task_key"].help_text
        assert "tasks" in help_text, "the module that is imported is not named"
        assert "SCHEDULABLE_TASKS" in help_text, (
            "the way to expose a task from any other module is not named"
        )


class TestReEnableThroughTheChangeForm:
    def test_the_boundary_moves(self, client, admin_user):
        # Through the form, not the action. save_model hands update_schedule
        # form.instance, which _post_clean has already updated, so reading
        # the previous value off that instance would never show a transition.
        row = a_schedule()
        update_schedule(row, enabled=False)
        paused_boundary = row.start_time
        client.force_login(admin_user)
        response = client.post(
            reverse(CHANGE_URL, args=[row.pk]),
            {
                "name": row.name,
                "task_key": row.task_key,
                "trigger": "cron",
                "cron": row.cron,
                "arguments": "{}",
                "phase_seconds": 0,
                "enabled": "on",
            },
        )
        assert response.status_code == 302
        row.refresh_from_db()
        assert row.enabled
        assert row.start_time > paused_boundary, (
            "re-enabling through the change form must move the boundary too"
        )


class TestDeletePermission:
    def test_delete_consults_the_registry_permission(self, client, staff_user):
        from django.contrib.admin.sites import site

        restricted = a_schedule(name="r", task_key="restricted")
        plain = a_schedule(name="plain")
        model_admin = site._registry[OxSchedule]
        request = client.request().wsgi_request
        request.user = staff_user
        assert not model_admin.has_delete_permission(request, restricted)
        assert model_admin.has_delete_permission(request, plain)


class TestTheAddFlowLeavesASavedObject:
    def test_save_and_continue_editing_goes_to_the_row(self, client, admin_user):
        # save_model routes creation through the service layer, which builds
        # and saves its own instance. Without binding the result back the
        # admin holds an object with no pk, and this redirect targets a URL
        # containing None.
        client.force_login(admin_user)
        response = client.post(
            reverse(ADD_URL),
            {
                "name": "nightly",
                "task_key": "report",
                "trigger": "cron",
                "cron": "0 2 * * *",
                "arguments": "{}",
                "phase_seconds": 0,
                "enabled": "on",
                "_continue": "Save and continue editing",
            },
        )
        row = OxSchedule.objects.get(name="nightly")
        assert response.status_code == 302
        assert "None" not in response["Location"]
        assert str(row.pk) in response["Location"]

    def test_the_admin_log_records_the_real_row(self, client, admin_user):
        from django.contrib.admin.models import LogEntry

        client.force_login(admin_user)
        client.post(
            reverse(ADD_URL),
            {
                "name": "nightly",
                "task_key": "report",
                "trigger": "cron",
                "cron": "0 2 * * *",
                "arguments": "{}",
                "phase_seconds": 0,
                "enabled": "on",
            },
        )
        row = OxSchedule.objects.get(name="nightly")
        entry = LogEntry.objects.latest("id")
        assert entry.object_id == str(row.pk), (
            "the row's admin history is attached to nothing"
        )


class TestAnEndTimeInThePastIsAFieldError:
    """
    `start_time` is not a form field, so Django leaves it out of the form's
    own validation and the pair is never compared on the way in. The
    service layer then sets it to now and refuses the row, which reaches
    the person filling the form in as a server error rather than as an
    error on the field they got wrong.
    """

    def _post(self, client, **over):
        fields = {
            "name": "nightly",
            "task_key": "report",
            "trigger": "cron",
            "cron": "0 2 * * *",
            "arguments": "{}",
            "phase_seconds": 0,
            "enabled": "on",
        }
        fields.update(over)
        return client.post(reverse(ADD_URL), fields)

    def test_adding_one_reports_the_field_rather_than_failing(self, client, admin_user):
        client.force_login(admin_user)
        yesterday = timezone.now() - timedelta(days=1)
        day, clock = _form_datetime(yesterday)
        response = self._post(client, end_time_0=day, end_time_1=clock)
        assert response.status_code == 200, "the form should be redisplayed"
        assert "end_time" in response.context["adminform"].form.errors
        assert not OxSchedule.objects.filter(name="nightly").exists()

    def test_a_future_end_time_is_accepted(self, client, admin_user):
        client.force_login(admin_user)
        tomorrow = timezone.now() + timedelta(days=1)
        day, clock = _form_datetime(tomorrow)
        response = self._post(client, end_time_0=day, end_time_1=clock)
        assert response.status_code == 302
        assert OxSchedule.objects.filter(name="nightly").exists()

    def test_an_existing_schedule_may_keep_a_past_end_time(self, client, admin_user):
        # Not every past end time is wrong: a schedule that ran and has
        # since ended holds one legitimately. Only a submission that also
        # moves the boundary to now makes the pair impossible.
        client.force_login(admin_user)
        row = a_schedule(name="ended")
        past = row.start_time + timedelta(hours=1)
        OxSchedule.objects.filter(pk=row.pk).update(end_time=past)
        day, clock = _form_datetime(past)
        response = client.post(
            reverse(CHANGE_URL, args=[row.pk]),
            {
                "name": "ended",
                "task_key": "report",
                "trigger": "cron",
                "cron": row.cron,
                "arguments": "{}",
                "phase_seconds": 0,
                "enabled": "on",
                "end_time_0": day,
                "end_time_1": clock,
            },
        )
        assert response.status_code == 302, "an unchanged timing must still save"


class TestDeletingThroughTheAdminTellsTheWorkers:
    """
    Both delete hooks reach into the service layer to bump the change
    marker. Without it a deleted schedule stays in every running worker's
    cache, enqueueing and rolling back once a pass.
    """

    def test_deleting_one_row_bumps_the_marker(self, client, admin_user):
        from django_ox.models import OxScheduleChange

        client.force_login(admin_user)
        row = a_schedule()
        before = OxScheduleChange.objects.get(id=1).changed_at
        response = client.post(
            reverse("admin:django_ox_oxschedule_delete", args=[row.pk]),
            {"post": "yes"},
        )
        assert response.status_code == 302
        assert not OxSchedule.objects.filter(pk=row.pk).exists()
        assert OxScheduleChange.objects.get(id=1).changed_at > before

    def test_deleting_a_selection_bumps_the_marker_once(self, client, admin_user):
        from django.contrib.admin.helpers import ACTION_CHECKBOX_NAME

        from django_ox.models import OxScheduleChange

        client.force_login(admin_user)
        rows = [a_schedule(name=f"s{i}") for i in range(3)]
        before = OxScheduleChange.objects.get(id=1).changed_at
        response = client.post(
            reverse("admin:django_ox_oxschedule_changelist"),
            {
                "action": "delete_selected",
                ACTION_CHECKBOX_NAME: [str(r.pk) for r in rows],
                "post": "yes",
            },
        )
        assert response.status_code == 302
        assert not OxSchedule.objects.exists()
        assert OxScheduleChange.objects.get(id=1).changed_at > before


class TestTheLastTickColumnReadsItsRow:
    """
    The Last tick column used to look the newest tick up once per listed
    schedule. The page already has the schedules, so the column now reads
    an annotation get_queryset adds, and the page's statement count does
    not move with the number of rows.
    """

    def _tick(self, row, at=None):
        at = timezone.now() - timedelta(hours=1) if at is None else at
        OxScheduleTick.objects.create(
            schedule_name=f"{STORED_KEY_PREFIX}{row.pk}",
            scheduled_for=at,
            created_at=at,
        )
        return at

    def test_a_tick_is_shown_and_a_row_without_ticks_says_never(
        self, client, admin_user
    ):
        ran = a_schedule(name="ran")
        a_schedule(name="quiet")
        at = self._tick(ran)
        client.force_login(admin_user)
        body = client.get(
            reverse("admin:django_ox_oxschedule_changelist")
        ).content.decode()
        assert f"{at:%Y-%m-%d %H:%M}" in body
        # The cell itself, not the word: the page can also carry the
        # warning about schedules that are "stored and never dispatched".
        assert ">never<" in body

    def test_the_newest_of_several_ticks_is_the_one_shown(self, client, admin_user):
        # One tick per schedule cannot tell newest from oldest, so the
        # ordering inside the subquery is only held here.
        ran = a_schedule(name="ran")
        old = self._tick(ran, timezone.now() - timedelta(days=3))
        new = self._tick(ran, timezone.now() - timedelta(hours=1))
        client.force_login(admin_user)
        body = client.get(
            reverse("admin:django_ox_oxschedule_changelist")
        ).content.decode()
        assert f"{new:%Y-%m-%d %H:%M}" in body
        assert f"{old:%Y-%m-%d %H:%M}" not in body

    def test_the_column_stops_costing_a_query_per_row(self, client, admin_user):
        for i in range(3):
            self._tick(a_schedule(name=f"s{i:03d}"))
        client.force_login(admin_user)

        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        with CaptureQueriesContext(connection) as few:
            first = client.get(reverse("admin:django_ox_oxschedule_changelist"))
        for i in range(30, 60):
            self._tick(a_schedule(name=f"s{i:03d}"))
        with CaptureQueriesContext(connection) as many:
            second = client.get(reverse("admin:django_ox_oxschedule_changelist"))
        # Equal counts on two failed pages would pass this on their own.
        assert first.status_code == 200
        assert second.status_code == 200
        assert "s000" in first.content.decode()
        assert "s059" in second.content.decode()
        assert len(many.captured_queries) == len(few.captured_queries), (
            "the Last tick column still spends a query per listed schedule"
        )
