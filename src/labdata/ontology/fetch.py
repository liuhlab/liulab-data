"""GET a vocabulary's file, or only its first bytes, through the HTTP transport seam.

Both follow redirects, which the transport does not, and retry a dropped connection or a busy
server. Every try starts again from the first address.
"""

from __future__ import annotations

import email.utils
import http.client
import os
import time
from collections.abc import Callable
from pathlib import Path

from labdata._files import temp_beside
from labdata._http import (
    RETRY_STATUSES,
    USER_AGENT,
    GaveUp,
    Response,
    TooManyRedirects,
    Transport,
    TryAgain,
    get_following,
    retry,
)
from labdata.exceptions import DownloadError

#: The bytes a header read asks for, and the most it reads.
HEAD_BYTES = 64 * 1024

_TRIES = 5
_TIMEOUT = 60.0
_REDIRECTS = 5
_CHUNK = 1 << 20

# Bound at import, so tests patch out the backoff sleep.
_sleep = time.sleep


def read_head(url: str, *, transport: Transport, size: int = HEAD_BYTES) -> bytes:
    """Return at most the first ``size`` bytes of the file at ``url``.

    Asks for a byte range, and reads no further than ``size`` when the server sends more.

    Raises
    ------
    DownloadError
        If every try fails, or the server refuses.
    """
    return _get(url, transport, {"Range": f"bytes=0-{size - 1}"}, lambda r: _upto(r, size))


def last_modified(url: str, *, transport: Transport) -> str | None:
    """Return the ``Last-Modified`` header of the file at ``url``, asking for one byte.

    Raises
    ------
    DownloadError
        If every try fails, or the server refuses.
    """
    return _get(url, transport, {"Range": "bytes=0-0"}, lambda r: r.getheader("Last-Modified"))


def download(url: str, dest: Path, *, transport: Transport) -> str | None:
    """Save the file at ``url`` to ``dest``, written whole, dated by its ``Last-Modified``.

    Returns
    -------
    str or None
        The response's ``Last-Modified`` header.

    Raises
    ------
    DownloadError
        If every try fails, or the server refuses.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    def save(response: Response) -> str | None:
        modified = response.getheader("Last-Modified")
        tmp = temp_beside(dest)
        try:
            with tmp.open("wb") as out:
                while chunk := response.read(_CHUNK):
                    out.write(chunk)
            _date(tmp, modified)
            tmp.replace(dest)
        finally:
            tmp.unlink(missing_ok=True)
        return modified

    return _get(url, transport, {}, save)


def _get[T](
    url: str, transport: Transport, extra: dict[str, str], take: Callable[[Response], T]
) -> T:
    headers = {"User-Agent": USER_AGENT, **extra}

    def once() -> T:
        target = url
        try:
            with get_following(
                transport, url, headers=headers, timeout=_TIMEOUT, hops=_REDIRECTS
            ) as (target, response):
                status = response.status
                if status in (200, 206):
                    return take(response)
                response.read()
        except TooManyRedirects as err:
            raise GaveUp(str(err)) from None
        except (OSError, ValueError, http.client.HTTPException) as err:
            raise TryAgain(f"{type(err).__name__} from {target}") from err
        if status in RETRY_STATUSES:
            raise TryAgain(f"HTTP {status} from {target}")
        raise GaveUp(f"HTTP {status} from {target}")

    try:
        return retry(once, tries=_TRIES, cap=60.0, sleep=_sleep)
    except GaveUp as err:
        raise DownloadError(f"GET {url} failed: {err}") from None


def _date(path: Path, modified: str | None) -> None:
    """Give a saved file the server's ``Last-Modified`` time, which then travels with it."""
    try:
        stamp = email.utils.parsedate_to_datetime(modified or "").timestamp()
    except ValueError:
        return
    os.utime(path, (stamp, stamp))


def _upto(response: Response, size: int) -> bytes:
    got = bytearray()
    while len(got) < size and (chunk := response.read(size - len(got))):
        got += chunk
    return bytes(got)
