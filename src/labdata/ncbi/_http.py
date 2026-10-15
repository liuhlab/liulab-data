"""The HTTP layer under :class:`~labdata.ncbi.entrez.EntrezClient`: kept-open connections and pacing."""

from __future__ import annotations

import http.client
import threading
import time
import urllib.parse
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import Protocol

# Bound at import, so tests that stub the client's backoff sleep keep the real pacing.
_sleep = time.sleep


class Response(Protocol):
    """The part of :class:`http.client.HTTPResponse` the client reads."""

    status: int

    def getheader(self, name: str) -> str | None:
        """Return one response header."""
        ...

    def read(self, amt: int | None = None) -> bytes:
        """Read up to ``amt`` bytes of the body, or all of it."""
        ...


class Transport(Protocol):
    """Sends one HTTP request; the seam tests replace."""

    def open(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> AbstractContextManager[Response]:
        """Send a request and yield its response for reading."""
        ...


class HttpTransport:
    """HTTP over connections kept open between requests, one per thread and host."""

    def __init__(self) -> None:
        self._local = threading.local()

    def close(self) -> None:
        """Close the calling thread's kept-open connections."""
        for conn in self._local.__dict__.pop("conns", {}).values():
            conn.close()

    @contextmanager
    def open(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> Iterator[Response]:
        """Send a request and yield its response; every socket wait is capped at ``timeout``."""
        parts = urllib.parse.urlsplit(url)
        conns: dict[tuple[str, str], http.client.HTTPConnection] = self._local.__dict__.setdefault(
            "conns", {}
        )
        key = (parts.scheme, parts.netloc)
        conn = conns.pop(key, None)
        if conn is None:
            cls = (
                http.client.HTTPSConnection
                if parts.scheme == "https"
                else http.client.HTTPConnection
            )
            conn = cls(parts.netloc, timeout=timeout)
        conn.timeout = timeout
        if conn.sock is not None:
            conn.sock.settimeout(timeout)
        path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        response: http.client.HTTPResponse | None = None
        try:
            conn.request(method, path, body=body, headers=dict(headers))
            response = conn.getresponse()
            yield response
        finally:
            # A body left unread, or a server that asked to close, makes the connection
            # unusable; a reply read in full keeps it, a 404 included.
            if response is None or response.will_close or not response.isclosed():
                conn.close()
            else:
                conns[key] = conn


class RateLimiter:
    """Paces calls to at most ``rate`` per second and caps calls in flight.

    Parameters
    ----------
    rate : float
        Calls per second.
    inflight : int
        Calls open at once.
    floor : float
        The lowest rate :meth:`halve` goes to.
    """

    def __init__(self, rate: float, inflight: int, *, floor: float = 0.5) -> None:
        self.rate = rate
        self.floor = floor
        self._next = 0.0
        self._lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(inflight)

    def lower_to(self, rate: float) -> None:
        """Cap the rate at ``rate``; never raises it."""
        with self._lock:
            self.rate = min(self.rate, rate)

    def halve(self) -> float:
        """Halve the rate, down to the floor, and return the new rate."""
        with self._lock:
            self.rate = max(self.floor, self.rate / 2)
            return self.rate

    @contextmanager
    def turn(self) -> Iterator[None]:
        """Hold an in-flight slot, wait for this call's start time, then run the call."""
        # The slot is taken before the start time, so slots freeing at once give no burst.
        with self._slots:
            with self._lock:
                now = time.monotonic()
                start = max(now, self._next)
                self._next = start + 1.0 / self.rate
            if start > now:
                _sleep(start - now)
            yield
