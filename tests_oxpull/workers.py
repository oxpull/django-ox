"""
Running real workers at each other, for the rate-limit tests.

racing.py races one callable; a limiter's subject is a Worker running its
claim loop against a shared budget, which that harness cannot express. So
this sits beside it rather than replacing it.
"""

import threading
import time

from django.db import connections

from oxpull.worker import OxpullWorker

from .racing import pool_for


def run_workers(
    count,
    *,
    seconds,
    worker_kwargs=None,
    worker_class=OxpullWorker,
    until_idle=False,
):
    """
    Run `count` worker_class instances against the queue at once, each on
    its own thread with its own connection, all released from one barrier,
    each claiming and executing until `seconds` have elapsed. Returns the
    monotonic instant of every claim, per worker.

    With until_idle the run ends when every worker is at rest on an empty
    queue instead, and `seconds` becomes a ceiling that a passing run never
    waits out and that fails the run when it is reached. That is the shape
    a test needs when it counts what the workers took before they stopped:
    what they took is the limiter's business, how long they needed is the
    machine's.

    A worker comes to rest when a claim found nothing and nothing was taken
    while that claim ran, and it stays there until another worker takes
    something. So the last worker to come to rest finds every other one
    already resting: no claim is in flight, each of them has been refused
    since the last task was taken, and a claim refused with no other claim
    in its way was refused by the limiter. That is the moment the run ends.
    Resting on the tally rather than on an empty claim alone is what makes
    it sound: a compare-and-set claim comes back empty when other workers
    took every candidate it had fetched, and a SKIP LOCKED claim when the
    rows it wanted were held by a claim still in flight, and both of those
    move the tally.

    Threads, not processes: a thread with its own connection is the same
    claimer to the database, and it is what a test can join. The W in the
    guarantee counts worker processes, which is why the bound assertions
    are written against the number of workers started here rather than
    against a constant.
    """
    kwargs = {"backoff_initial": 0, "poll_interval": 0.01, **(worker_kwargs or {})}
    # Each worker process has a connection pool of its own. These threads
    # share this process's, so it is sized for them.
    with pool_for(count):
        return _run_workers(count, seconds, kwargs, worker_class, until_idle)


def _run_workers(count, seconds, kwargs, worker_class, until_idle):
    barrier = threading.Barrier(count)
    claims = [[] for _ in range(count)]
    quiet = threading.Condition()
    # True while a worker is waiting out a claim that found nothing on a
    # tally that did not move while it ran.
    resting = [False] * count
    taken = 0
    idle = threading.Event()
    errors = []

    def run(index):
        nonlocal taken
        try:
            worker = worker_class(**kwargs)
            barrier.wait(timeout=10)
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline and not idle.is_set():
                with quiet:
                    began_at = taken
                db_task = worker.claim_one()
                if db_task is None:
                    with quiet:
                        if until_idle and began_at == taken:
                            resting[index] = True
                            if all(resting):
                                idle.set()
                                quiet.notify_all()
                                break
                            woken = quiet.wait_for(
                                lambda: idle.is_set() or not resting[index],
                                timeout=max(0.0, deadline - time.monotonic()),
                            )
                            if not woken:
                                # Out of time, not back at work: leave the
                                # run one worker short of idle so it fails.
                                resting[index] = False
                            continue
                    # Idle rather than spin: the point of the run is what
                    # the limiter admits, not how hard a blocked worker
                    # can hammer the database.
                    time.sleep(0.005)
                    continue
                claims[index].append(time.monotonic())
                with quiet:
                    taken += 1
                    # What every resting worker is waiting for.
                    resting[:] = [False] * count
                    quiet.notify_all()
                worker.execute(db_task)
        except BaseException as exc:
            errors.append(exc)
        finally:
            connections.close_all()

    threads = [threading.Thread(target=run, args=(index,)) for index in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=seconds + 30)
        assert not thread.is_alive()
    assert not errors, errors
    if until_idle:
        assert idle.is_set(), f"the workers were still claiming after {seconds}s"
    return claims


def admissions(claims):
    """Every claim across every worker, in the order they happened."""
    return sorted(instant for worker in claims for instant in worker)
