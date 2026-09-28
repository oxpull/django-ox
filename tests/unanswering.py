"""
A TCP relay in front of the test database that can stop answering, for the
look a stopping worker makes (tests/test_claim_recovery.py).

black_hole(): every connection made from then on is accepted and never
answered, as by a server that has gone away behind a live address. stall():
connections already open stop relaying, in both directions, as a server
that stops answering once connected; what a client sends from then on is
counted and dropped, so a test can tell its statement went out and was
never answered. stall_open(): the same for the connections open at the
call only; one made later is relayed until it is stalled in turn, as a
server that accepts and sets up a session and then answers nothing, or a
pool's idle connections to a host that went away without resetting them.
"""

import socket
import threading
from contextlib import suppress


class Unanswering:
    def __init__(self, host, port):
        self._upstream = (host, int(port))
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self._black_hole = threading.Event()
        self._stalled = threading.Event()
        self._lock = threading.Lock()
        self._sockets = []
        #: One flag per relayed connection, set by stall_open().
        self._each_stalled = []
        #: Connections accepted and never answered.
        self.unanswered_connects = 0
        #: Bytes a client sent while relaying was stalled, none of them passed on.
        self.unanswered_bytes = 0
        threading.Thread(target=self._accept, daemon=True).start()

    def black_hole(self):
        self._black_hole.set()

    def stall(self):
        self._stalled.set()

    def stall_open(self):
        with self._lock:
            for stalled in self._each_stalled:
                stalled.set()

    def close(self):
        """Reset every connection, which wakes whatever waits on one."""
        with suppress(OSError):
            self._listener.shutdown(socket.SHUT_RDWR)
        with suppress(OSError):
            self._listener.close()
        with self._lock:
            sockets = list(self._sockets)
        for sock in sockets:
            with suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            with suppress(OSError):
                sock.close()

    def _keep(self, sock):
        with self._lock:
            self._sockets.append(sock)

    def _accept(self):
        while True:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            self._keep(client)
            if self._black_hole.is_set():
                with self._lock:
                    self.unanswered_connects += 1
                continue
            try:
                upstream = socket.create_connection(self._upstream)
            except OSError:
                client.close()
                continue
            self._keep(upstream)
            stalled = threading.Event()
            with self._lock:
                self._each_stalled.append(stalled)
            for source, sink, from_client in (
                (client, upstream, True),
                (upstream, client, False),
            ):
                threading.Thread(
                    target=self._pipe,
                    args=(source, sink, from_client, stalled),
                    daemon=True,
                ).start()

    def _pipe(self, source, sink, from_client, stalled):
        while True:
            try:
                data = source.recv(65536)
            except OSError:
                return
            if not data:
                with suppress(OSError):
                    sink.shutdown(socket.SHUT_WR)
                return
            if self._stalled.is_set() or stalled.is_set():
                if from_client:
                    with self._lock:
                        self.unanswered_bytes += len(data)
                continue
            try:
                sink.sendall(data)
            except OSError:
                return
