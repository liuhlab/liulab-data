"""A fake web for the ontology update: canned files, redirects and failures at the transport seam.

It records each request with its headers, and how many body bytes each response gave, so a
test can show what was downloaded and when.
"""

from __future__ import annotations

import io
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field

_RANGE = re.compile(r"bytes=(\d+)-(\d*)")


@dataclass
class Call:
    """One request the fake web answered, and the body bytes its response gave."""

    method: str
    url: str
    headers: dict[str, str]
    status: int = 0
    given: int = 0


class _Reply:
    """One response; counts the bytes read into its call."""

    def __init__(self, call: Call, body: bytes, status: int, headers: Mapping[str, str]) -> None:
        self._call = call
        self._body = io.BytesIO(body)
        self.status = call.status = status
        self.will_close = False
        self._headers = {k.lower(): v for k, v in headers.items()}

    def getheader(self, name: str) -> str | None:
        return self._headers.get(name.lower())

    def read(self, amt: int | None = None) -> bytes:
        chunk = self._body.read(amt)
        self._call.given += len(chunk)
        return chunk

    def isclosed(self) -> bool:
        return True


@dataclass
class OntologyWeb:
    """Serves ``files`` by URL; ``redirects`` send a URL on with a 302.

    ``failures`` lists, per URL, statuses to answer before serving it. A URL in
    ``no_ranges`` answers a Range request with the whole file, as a server that ignores
    Range does. ``modified`` gives a URL's ``Last-Modified`` header.
    """

    files: dict[str, bytes] = field(default_factory=dict)
    redirects: dict[str, str] = field(default_factory=dict)
    failures: dict[str, list[int]] = field(default_factory=dict)
    no_ranges: set[str] = field(default_factory=set)
    modified: dict[str, str] = field(default_factory=dict)
    calls: list[Call] = field(default_factory=list)

    @contextmanager
    def open(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> Iterator[_Reply]:
        call = Call(method, url, dict(headers))
        self.calls.append(call)
        if self.failures.get(url):
            yield _Reply(call, b"busy", self.failures[url].pop(0), {})
        elif url in self.redirects:
            yield _Reply(call, b"moved", 302, {"Location": self.redirects[url]})
        elif url in self.files:
            yield self._serve(call, self.files[url], headers.get("Range"))
        else:
            yield _Reply(call, b"not found", 404, {})

    def _serve(self, call: Call, data: bytes, wanted: str | None) -> _Reply:
        match = _RANGE.fullmatch(wanted or "")
        dated = {"Last-Modified": self.modified[call.url]} if call.url in self.modified else {}
        if match is None or call.url in self.no_ranges:
            return _Reply(call, data, 200, {"Content-Length": str(len(data)), **dated})
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else len(data) - 1
        part = data[start : end + 1]
        content_range = f"bytes {start}-{start + len(part) - 1}/{len(data)}"
        return _Reply(call, part, 206, {"Content-Range": content_range, **dated})

    def gets(self, url: str) -> list[Call]:
        """Return the GETs of ``url``, in order."""
        return [c for c in self.calls if c.method == "GET" and c.url == url]
