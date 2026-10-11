"""
Running one callable on many threads at once.

Shared by the unique and batch race tests: both prove the same class of
property (a rowcount, not a check-then-act, picks the single winner), so
they share the harness rather than two copies of it.
"""

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

THREADS = 8


@contextmanager
def pool_for(threads: int) -> Iterator[None]:
    """
    On a pooled database, room in this process's pool for `threads`
    connections at once beside the test's own.

    Each thread here stands for a process or a request with a connection of
    its own. These threads share one process's pool instead, so it is sized
    for them, as a worker's pool is sized for its threads (django-ox
    docs/production.md). A smaller pool makes a thread wait for a
    connection rather than for the lock or row the test is about.
    """
    from django.db import connections

    pool = getattr(connections["default"], "pool", None)
    if pool is None or pool.max_size >= threads + 1:
        yield
        return
    sizes = (pool.min_size, pool.max_size)
    pool.resize(pool.min_size, threads + 1)
    try:
        yield
    finally:
        pool.resize(*sizes)


def race(
    target: Callable[[], Any], threads: int = THREADS
) -> tuple[list[Any], list[BaseException]]:
    """The same callable on `threads` threads at once."""
    return race_all([target] * threads)


def race_all(
    targets: list[Callable[[], Any]],
) -> tuple[list[Any], list[BaseException]]:
    """
    Call each target on its own thread, all released from one barrier,
    and return (results, errors). Used with django_db(transaction=True)
    so each thread's connection sees committed state, mirroring real
    worker processes.
    """
    from django.db import connections

    threads = len(targets)
    barrier = threading.Barrier(threads)
    results: list[Any] = []
    errors: list[BaseException] = []

    def run(target: Callable[[], Any]) -> None:
        try:
            barrier.wait(timeout=10)
            results.append(target())
        except BaseException as exc:
            errors.append(exc)
        finally:
            # Threads get their own connections; close them like the
            # worker's pool threads do.
            connections.close_all()

    workers = [threading.Thread(target=run, args=(target,)) for target in targets]
    with pool_for(threads):
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=30)
            assert not worker.is_alive()
    return results, errors
