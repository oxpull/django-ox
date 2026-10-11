"""
Workflow.add() inside a caller's transaction reads a stale view.

add() takes the workflow row lock with an UPDATE, which is a current read,
and then resolves depends_on and reads its upstreams' outcomes. Under
REPEATABLE READ a plain SELECT inside a caller's transaction answers from
the read view that transaction's first consistent read fixed, not from the
state under the lock. A node, or an upstream outcome, committed after that
first read and before add() is invisible to it:

* _resolve_dependencies would raise UnknownWorkflowNode for a node that
  exists.
* add() would leave a node WAITING whose upstream has already succeeded and
  already ran its release, so the node would wait for a reconciler tick.

Django's MySQL and PostgreSQL backends both default to READ COMMITTED, where
each statement reads current state. A project reaches REPEATABLE READ by
setting DATABASES[...]["OPTIONS"]["isolation_level"], or None on MySQL for
the server's default. The level is set on the caller's session here, as
tests_oxpull/test_batches.py and tests_oxpull/test_unique.py do, and READ COMMITTED runs
the same sequence as the control.

add() resolves depends_on with a locking read, which is a current read on
MySQL, and when it leaves a node WAITING it checks the node's upstreams
again once its transaction commits. add() also locks the upstream's node
row, and a worker recording that upstream's outcome locks the same row. So
in the last test, an upstream that finishes while the adding transaction is
still open waits for its commit, and then marks the new edge and releases
the node.

Every test asserts the correct behaviour, and every test passes.
"""

import threading
from contextlib import contextmanager

import pytest
from django.db import OperationalError, connections, transaction

import oxpull.worker as oxpull_worker
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.backend import is_serialization_failure
from oxpull.exceptions import UnknownWorkflowNode
from oxpull.models import OxpullWorkflowEdge

from . import isolation, tasks
from .racing import race_all
from .test_workflows import order
from .test_workflows_deferred import deferring_worker, status
from .test_workflows_hazard_postgresql_repeatable_read_concurrent_add import (
    wait_for_lock_waiters,
)

pytestmark = pytest.mark.django_db(transaction=True)

LEVELS = ["READ COMMITTED", "REPEATABLE READ"]

VENDOR = connections["default"].vendor


def skip_where_no_concurrent_commit_fits():
    if connections["default"].vendor == "sqlite":
        pytest.skip(
            "a reading SQLite transaction holds its shared lock, so the "
            "concurrent commit this hazard needs cannot land under it"
        )


@contextmanager
def caller_session_at(level):
    """
    The caller's session at `level` for the block, on MySQL or PostgreSQL.
    The connection is closed afterwards so the next use reopens it at the
    configured level.
    """
    connection = connections["default"]
    if connection.vendor == "mysql":
        statement = f"SET SESSION TRANSACTION ISOLATION LEVEL {level}"
    else:
        statement = (
            f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}"
        )
    isolation.outlives_the_transaction(level)
    with connection.cursor() as cursor:
        cursor.execute(statement)
    try:
        yield
    finally:
        connection.close()


@pytest.mark.parametrize("level", LEVELS)
def test_add_resolves_a_dependency_committed_after_the_callers_first_read(level):
    """
    _resolve_dependencies promises that a concurrent add() of the named
    node "commits before this transaction gets the lock, or after this one
    releases it, so the name either resolves or does not". Here stage-2
    commits before this add() takes the lock, so it must resolve.

    The caller's first read is workflows.get(), what a view running under
    ATOMIC_REQUESTS does before it adds a node. Another connection adds
    stage-2 and commits. The caller then adds stage-3 depending on it.

    PostgreSQL REPEATABLE READ never reaches the stale lookup: the other
    add() rewrote the workflow row after the caller's snapshot, so the
    caller's lock UPDATE raises a serialization failure, which is that
    level's documented and retryable refusal.
    """
    skip_where_no_concurrent_commit_fits()
    if connections["default"].vendor == "postgresql" and level == "REPEATABLE READ":
        pytest.skip(
            "PostgreSQL REPEATABLE READ refuses the workflow-row UPDATE with a "
            "serialization failure before any name is looked up"
        )
    flow = workflows.create("resolve")
    flow.add("root", tasks.step, "root")

    def another_request_adds_stage_2():
        return (
            workflows.get(flow.id)
            .add("stage-2", tasks.step, "stage-2", depends_on="root")
            .id
        )

    with caller_session_at(level), transaction.atomic():
        # The caller's first consistent read. Under REPEATABLE READ the
        # read view is fixed here.
        handle = workflows.get(flow.id)
        results, errors = race_all([another_request_adds_stage_2])
        assert errors == []
        (stage_2_id,) = results
        try:
            stage_3 = handle.add("stage-3", tasks.step, "stage-3", depends_on="stage-2")
        except UnknownWorkflowNode as exc:
            pytest.fail(
                f"add() raised {exc!r}, but stage-2 (task {stage_2_id}) had "
                "committed before add() took the workflow row lock"
            )

    edge = OxpullWorkflowEdge.objects.get(downstream_task_id=stage_3.id)
    assert str(edge.upstream_task_id) == stage_2_id


@pytest.mark.parametrize("level", LEVELS)
def test_a_node_whose_upstream_succeeded_behind_the_callers_snapshot_is_not_deferred(
    level, monkeypatch
):
    """
    add() releases a node whose upstreams have all succeeded
    (test_a_node_whose_parent_already_succeeded_is_released_as_it_is_added).
    That matters because the one release an upstream makes happens when its
    outcome lands. A node left WAITING after that waits for a reconciler
    tick, and without a scheduled reconciler it waits forever.

    The caller reads first. A real worker on another connection then runs
    the root to SUCCESSFUL and makes its release, which finds nothing to
    release because the child does not exist yet. Then the caller adds the
    child.
    """
    skip_where_no_concurrent_commit_fits()
    releases = []
    real_release = oxpull_worker.release_downstream

    def recording_release(task_ids, db_alias):
        released = real_release(task_ids, db_alias)
        releases.append((sorted(str(task_id) for task_id in task_ids), released))
        return released

    monkeypatch.setattr(oxpull_worker, "release_downstream", recording_release)

    flow = workflows.create("defer")
    root = flow.add("root", tasks.deferred_step, "root")

    def a_worker_runs_the_root():
        return deferring_worker().run_once()

    refusals = []
    with caller_session_at(level):
        for attempt in range(2):
            try:
                with transaction.atomic():
                    handle = workflows.get(flow.id)
                    if attempt == 0:
                        results, errors = race_all([a_worker_runs_the_root])
                        assert (results, errors) == ([True], [])
                    handle.add("child", tasks.deferred_step, "child", depends_on="root")
                break
            except OperationalError as exc:
                if attempt or not is_serialization_failure(exc, "default"):
                    raise
                refusals.append(exc)

    # The root's outcome rewrote its node row, which add() locks as the
    # child's upstream. At PostgreSQL REPEATABLE READ that row changed after
    # the caller's snapshot, so add() refuses with a serialization failure,
    # which is that level's documented and retryable refusal. The caller runs
    # its transaction again, on a snapshot that sees the root succeeded.
    refused = VENDOR == "postgresql" and level == "REPEATABLE READ"
    assert len(refusals) == (1 if refused else 0), refusals

    # The upstream had succeeded and made its one release before add() ran.
    # No edge pointed out of it then, so there was nothing to release.
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.SUCCESSFUL
    assert releases == [([], 0)]
    assert status("child") == OxTask.Status.READY, (
        "the child was left WAITING behind an upstream that had already "
        "succeeded and released; only a reconciler tick can release it now"
    )
    assert deferring_worker().run_once() is True
    assert order() == ["root", "child"]


@pytest.mark.parametrize("level", LEVELS)
def test_a_node_whose_upstream_succeeds_before_the_adding_transaction_commits_runs(
    level, monkeypatch
):
    """
    The same liveness at any isolation level. The caller adds the child
    while the root is READY, so add() rightly leaves it WAITING. A real
    worker then runs the root while the caller's transaction is still open.
    add() locked the root's node row, so the root's outcome waits for the
    caller's commit and then marks the child's edge. The child must be
    released when the caller commits, not a reconciler tick later.

    Two releases run once the caller commits, and either may move the child:
    the root worker's, after its outcome commits, and add()'s check after its
    own commit, which releases the child if it reads the edge marked. Which
    one wins is timing. The test asserts that the worker released after its
    outcome, and that exactly one of the two moved the child.
    """
    skip_where_no_concurrent_commit_fits()
    releases = []
    checks = []
    real_release = oxpull_worker.release_downstream

    def recording_release(task_ids, db_alias):
        released = real_release(task_ids, db_alias)
        releases.append((sorted(str(task_id) for task_id in task_ids), released))
        return released

    def recording_check(task_ids, db_alias):
        released = real_release(task_ids, db_alias)
        checks.append((sorted(str(task_id) for task_id in task_ids), released))
        return released

    monkeypatch.setattr(oxpull_worker, "release_downstream", recording_release)
    monkeypatch.setattr(workflows, "release_downstream", recording_check)

    flow = workflows.create("defer-late")
    root = flow.add("root", tasks.deferred_step, "root")

    def a_worker_runs_the_root():
        return deferring_worker().run_once()

    box = {}

    def run_the_root_in_the_background():
        try:
            box["ran"] = a_worker_runs_the_root()
        except BaseException as exc:
            box["error"] = exc
        finally:
            connections.close_all()

    runner = threading.Thread(target=run_the_root_in_the_background)
    with caller_session_at(level), transaction.atomic():
        child = workflows.get(flow.id).add(
            "child", tasks.deferred_step, "child", depends_on="root"
        )
        assert status("child") == OxTask.Status.WAITING, "the root had not run yet"
        runner.start()
        # Polled from its own connection: a snapshot of lock waits taken
        # inside the caller's transaction would not change while it is open.
        (waited,), poll_errors = race_all([lambda: wait_for_lock_waiters(1)])
        assert poll_errors == []
    runner.join(60)

    assert not runner.is_alive()
    assert box == {"ran": True}, box
    assert waited >= 1, "the root's outcome never waited on the adding transaction"
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.SUCCESSFUL
    assert [ids for ids, _ in releases] == [[str(child.id)]]
    assert [ids for ids, _ in checks] == [[str(child.id)]]
    assert sum(moved for _, moved in releases + checks) == 1, (releases, checks)
    assert status("child") == OxTask.Status.READY, (
        "the child stayed WAITING after its upstream succeeded and the adding "
        "transaction committed; only a reconciler tick can release it now"
    )
    assert deferring_worker().run_once() is True
    assert order() == ["root", "child"]
