"""
The admin registration: opt-in through django.contrib.admin's autodiscover,
read-only, with the two actions wired to django_ox.actions.
"""

import pytest
from django.contrib.auth.models import Permission, User
from django.urls import reverse
from django.utils import timezone

from django_ox.models import OxTask

from .tasks import STATE, add, fail_always


@pytest.fixture
def admin_client(client):
    user = User.objects.create_superuser("ops", "ops@example.com", "pw")
    client.force_login(user)
    return client


def failed_task(worker):
    fail_always.enqueue()
    for _ in range(3):
        OxTask.objects.update(run_after=None)
        worker.run_once()
    db_task = OxTask.objects.get()
    assert db_task.status == OxTask.Status.FAILED
    return db_task


CHANGELIST = "admin:django_ox_oxtask_changelist"


@pytest.mark.django_db
class TestChangelist:
    def test_lists_filters_and_searches(self, admin_client, worker):
        failed = failed_task(worker)
        ready = add.enqueue(1, 2)

        response = admin_client.get(reverse(CHANGELIST))
        assert response.status_code == 200
        body = response.content.decode()
        assert "tests.tasks.fail_always" in body
        assert "tests.tasks.add" in body
        assert str(failed.pk) in body
        assert "Retry selected tasks" in body
        assert "Discard selected tasks" in body

        response = admin_client.get(reverse(CHANGELIST), {"status__exact": "FAILED"})
        body = response.content.decode()
        assert "tests.tasks.fail_always" in body
        assert "tests.tasks.add" not in body

        response = admin_client.get(reverse(CHANGELIST), {"q": str(ready.id)})
        body = response.content.decode()
        assert "tests.tasks.add" in body
        assert "tests.tasks.fail_always" not in body

    def test_detail_is_read_only_and_shows_every_traceback(self, admin_client, worker):
        failed = failed_task(worker)
        url = reverse("admin:django_ox_oxtask_change", args=[failed.pk])

        response = admin_client.get(url)
        assert response.status_code == 200
        body = response.content.decode()
        assert body.count("ValueError: boom") == 3
        assert "Attempt 3: builtins.ValueError" in body
        assert 'name="task_path"' not in body  # no editable inputs
        assert 'name="status"' not in body

        # A POST to the change view is refused; the row is not editable.
        response = admin_client.post(url, {"status": "READY"})
        assert response.status_code == 403
        assert OxTask.objects.get().status == OxTask.Status.FAILED

    def test_add_and_delete_are_off(self, admin_client):
        response = admin_client.get(reverse("admin:django_ox_oxtask_add"))
        assert response.status_code == 403


@pytest.mark.django_db
class TestActions:
    def test_retry_selected_reports_counts(self, admin_client, worker):
        failed = failed_task(worker)
        ready = add.enqueue(1, 2)

        response = admin_client.post(
            reverse(CHANGELIST),
            {
                "action": "retry_selected",
                "_selected_action": [str(failed.pk), ready.id],
            },
            follow=True,
        )
        messages = [str(m) for m in response.context["messages"]]
        assert "Retried 1 task(s)." in messages
        assert "Skipped 1 task(s) whose status did not allow it." in messages

        failed.refresh_from_db()
        assert failed.status == OxTask.Status.READY
        assert failed.max_attempts == 4

        assert worker.run_once() is True
        failed.refresh_from_db()
        assert failed.attempts == 4

    def test_discard_selected_reports_counts_and_never_runs(self, admin_client, worker):
        claimed_result = add.enqueue(3, 4)
        worker.claim_one()
        ready = add.enqueue(1, 2)

        response = admin_client.post(
            reverse(CHANGELIST),
            {
                "action": "discard_selected",
                "_selected_action": [ready.id, claimed_result.id],
            },
            follow=True,
        )
        messages = [str(m) for m in response.context["messages"]]
        assert "Discarded 1 task(s)." in messages
        assert "Skipped 1 task(s) whose status did not allow it." in messages

        assert OxTask.objects.get(pk=ready.id).status == OxTask.Status.DISCARDED
        assert OxTask.objects.get(pk=claimed_result.id).status == OxTask.Status.RUNNING
        assert worker.run_once() is False
        assert STATE == {}

    def test_actions_need_the_change_permission(self, client):
        user = User.objects.create_user("viewer", password="pw", is_staff=True)
        user.user_permissions.add(Permission.objects.get(codename="view_oxtask"))
        client.force_login(user)
        failed = OxTask.objects.create(
            task_path="tests.tasks.add",
            backend_name="default",
            status=OxTask.Status.FAILED,
            enqueued_at="2026-01-01T00:00:00Z",
        )

        response = client.get(reverse(CHANGELIST))
        assert response.status_code == 200
        assert "Retry selected tasks" not in response.content.decode()

        response = client.post(
            reverse(CHANGELIST),
            {"action": "retry_selected", "_selected_action": [str(failed.pk)]},
            follow=True,
        )
        assert OxTask.objects.get().status == OxTask.Status.FAILED

    def test_select_across_is_a_few_queries_in_one_transaction(self, admin_client):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        now = timezone.now()
        OxTask.objects.bulk_create(
            [
                OxTask(
                    task_path="tests.tasks.add",
                    args=[1, 2],
                    backend_name="default",
                    status=OxTask.Status.READY if i % 2 else OxTask.Status.SUCCESSFUL,
                    enqueued_at=now,
                )
                for i in range(20000)
            ],
            batch_size=1000,
        )
        with CaptureQueriesContext(connection) as ctx:
            response = admin_client.post(
                reverse(CHANGELIST),
                {
                    "action": "discard_selected",
                    "select_across": "1",
                    "index": "0",
                    "_selected_action": [str(OxTask.objects.first().pk)],
                },
                follow=True,
            )
        messages = [str(m) for m in response.context["messages"]]
        assert "Discarded 10000 task(s)." in messages
        assert "Skipped 10000 task(s) whose status did not allow it." in messages
        # 20 UPDATEs for 20,000 rows; the rest is the admin's own requests.
        updates = [q for q in ctx.captured_queries if q["sql"].startswith("UPDATE")]
        assert len(updates) == 20
        assert len(ctx) < 60
        assert OxTask.objects.filter(status=OxTask.Status.DISCARDED).count() == 10000
