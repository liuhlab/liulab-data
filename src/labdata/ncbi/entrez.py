"""The one seam for NCBI E-utilities and the GEO download site.

Every NCBI call in liulab-data goes through :class:`EntrezClient`. E-utilities calls are
POSTs over kept-open connections, paced by one limiter per process; Biopython's
:func:`Bio.Entrez.read` parses the XML replies. ``fetch_text`` and ``download`` GET pages and
files outside E-utilities. Every request has a socket timeout and is retried with jittered
backoff when the failure is transient.

Responses are memoized per client instance, except ``efetch_records``, ``download`` and
``stream``, which bulk runs use.
"""

from __future__ import annotations

import contextlib
import gzip
import http
import http.client
import io
import random
import socket
import time
import urllib.error
import urllib.parse
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from email.message import Message
from pathlib import Path
from typing import Any, TypeVar

from Bio import Entrez

from labdata.exceptions import EntrezError, PageNotFound
from labdata.ncbi._http import HttpTransport, RateLimiter, Response, Transport
from labdata.ncbi.config import NcbiCredentials, load_credentials

# Bio.Entrez ships no type stubs; alias it as Any so the type checker doesn't fight it.
_entrez: Any = Entrez
_T = TypeVar("_T")

_EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

#: Maximum UIDs per batched esummary request.
_ESUMMARY_CHUNK = 200

#: Tries per request (one initial try plus retries).
_MAX_TRIES = 6

#: Tries of a 4xx other than 404 and 429: NCBI sometimes refuses a request it answers when
#: repeated. "ID list is empty" is never retried.
_REFUSED_TRIES = 2

#: Socket timeouts in seconds.
_EUTILS_TIMEOUT = 30.0
_GEO_TIMEOUT = 15.0

#: Bytes per read when streaming a download to disk.
_CHUNK_BYTES = 1 << 20

#: NCBI's cap without an API key, in requests per second.
_KEYLESS_RATE = 3.0

_USER_AGENT = "liulab-data (+https://github.com/liuhlab/liulab-data)"
_TOOL = "liulab-data"
_THROTTLED = (429, 503)
_EMPTY_IDS = "ID list is empty"

#: One limiter per process paces every E-utilities call of every client.
EUTILS_LIMITER = RateLimiter(5.0, 8)

_TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    http.client.IncompleteRead,
    http.client.RemoteDisconnected,
    ConnectionError,
    TimeoutError,
    socket.timeout,
    EOFError,
)

#: Substrings marking a transient drop when it reaches us as a bare ``RuntimeError``.
#: Bio.Entrez raises ``RuntimeError`` both for NCBI's own logical errors ("ID list is
#: empty") and when a reply ends mid-parse; only the latter is retried.
_TRANSIENT_MESSAGES: tuple[str, ...] = (
    "ended prematurely",
    "response ended",
    "read failed",
    "unexpectedly closed",
    "connection reset",
    "timed out",
    "incompleteread",
)


class _StatusError(urllib.error.HTTPError):
    """A non-200 reply, carrying its body."""

    def __init__(self, url: str, status: int, headers: Message, body: bytes) -> None:
        try:
            reason = http.HTTPStatus(status).phrase
        except ValueError:
            reason = "Unknown"
        super().__init__(url, status, reason, headers, io.BytesIO(body))
        self.body = body


def _is_transient(err: BaseException) -> bool:
    """Decide whether a failure is a retryable drop or the query's own permanent fault."""
    if isinstance(err, urllib.error.HTTPError):
        return err.code == 429 or err.code >= 500
    if isinstance(err, urllib.error.URLError):
        return True
    if isinstance(err, _TRANSIENT_EXCEPTIONS):
        return True
    return any(marker in str(err).lower() for marker in _TRANSIENT_MESSAGES)


def _ids_empty(err: BaseException) -> bool:
    """Tell NCBI's "ID list is empty" answer (no requested id resolves) from other failures."""
    body = getattr(err, "body", b"")
    return _EMPTY_IDS.encode() in body or _EMPTY_IDS in str(err)


def _backoff(attempt: int) -> float:
    """Exponential backoff with jitter, capped at 60 s."""
    return min(60.0, 2.0**attempt) * random.uniform(0.5, 1.0)


def _retry_after(err: BaseException) -> float:
    """Return the seconds a throttled reply asked us to wait, or zero."""
    headers = getattr(err, "headers", None)
    value = headers.get("Retry-After") if headers is not None else None
    with contextlib.suppress(TypeError, ValueError):
        return max(0.0, float(value))  # type: ignore[arg-type]
    return 0.0


def _read_xml(response: Response) -> Any:
    return _entrez.read(io.BytesIO(response.read()))


def _read_text(response: Response) -> str:
    data = response.read()
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return data.decode()


@dataclass
class FetchedRecords:
    """What a batched :meth:`EntrezClient.efetch_records` got back.

    Attributes
    ----------
    records : dict of str to str
        Each requested id mapped to its record text.
    missing : list of str
        Ids NCBI answered with "ID list is empty": it has no record for them.
    failed : dict of str to str
        Ids whose re-ask failed for another reason, mapped to the error; worth a later try.
    """

    records: dict[str, str] = field(default_factory=dict)
    missing: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)


def _docsums(record: Any) -> list[Any]:
    """Normalize an esummary parse to a list of document summaries.

    Classic esummary (``gds``, ``sra``) parses to a list of summaries; the v2.0
    ``DocumentSummarySet`` format (e.g. ``bioproject``) nests them one level
    down under ``DocumentSummarySet -> DocumentSummary``.
    """
    if isinstance(record, dict):
        summary_set = record.get("DocumentSummarySet")
        if summary_set is not None:
            return list(summary_set.get("DocumentSummary", []))
        return [record]
    return list(record)


def docsum_uid(docsum: Any) -> str | None:
    """Return the NCBI UID of a single document summary, if present.

    Classic ``DocSum`` records (``gds``/``sra``) carry the UID under the ``Id``
    key; v2.0 ``DocumentSummary`` records (``bioproject``) carry it as the XML
    ``uid`` attribute, which Biopython exposes via ``.attributes``.

    Parameters
    ----------
    docsum : Any
        One summary from :meth:`EntrezClient.esummary_many`.

    Returns
    -------
    str or None
        The UID, or ``None`` when the summary carries none.
    """
    uid = docsum.get("Id") if hasattr(docsum, "get") else None
    if not uid:
        attributes = getattr(docsum, "attributes", None) or {}
        uid = attributes.get("uid")
    return str(uid) if uid else None


def _chunked(items: list[str], size: int) -> list[list[str]]:
    """Split ``items`` into consecutive chunks of at most ``size`` elements."""
    return [items[i : i + size] for i in range(0, len(items), size)]


class EntrezClient:
    """Authenticated entry point to NCBI E-utilities and the GEO download site.

    Parameters
    ----------
    credentials : NcbiCredentials or None
        Credentials to use. When ``None`` (default) they are resolved via
        :func:`labdata.ncbi.config.load_credentials`.
    allow_prompt : bool
        Forwarded to :func:`load_credentials` when ``credentials`` is ``None``.
    transport : Transport or None
        Sends the HTTP requests; kept-open connections by default.
    limiter : RateLimiter or None
        Paces E-utilities calls; the process-wide :data:`EUTILS_LIMITER` by default. A 429
        or 503 halves its rate. Without an API key it is capped at NCBI's keyless limit.
    """

    def __init__(
        self,
        credentials: NcbiCredentials | None = None,
        *,
        allow_prompt: bool = True,
        transport: Transport | None = None,
        limiter: RateLimiter | None = None,
    ) -> None:
        self.credentials = credentials or load_credentials(allow_prompt=allow_prompt)
        self._transport: Transport = transport or HttpTransport()
        self._limiter = limiter or EUTILS_LIMITER
        self._identity = {"tool": _TOOL, "email": self.credentials.email}
        if self.credentials.api_key:
            self._identity["api_key"] = self.credentials.api_key
        else:
            self._limiter.lower_to(_KEYLESS_RATE)
        # Per-instance response cache keyed by call signature. Redundant lookups
        # across records that share this client are served without a request.
        self._cache: dict[tuple[Any, ...], Any] = {}

    def clear_cache(self) -> None:
        """Drop all cached responses (forcing fresh requests on the next call)."""
        self._cache.clear()

    def esearch(self, db: str, term: str, *, retmax: int = 20) -> list[str]:
        """Run an ESearch and return the matching UIDs.

        Parameters
        ----------
        db : str
            Entrez database to search (e.g. ``"gds"``).
        term : str
            The search term.
        retmax : int
            Maximum number of UIDs to return.

        Returns
        -------
        list of str
            The matching UIDs, in NCBI's ranking order. Cached per client.
        """
        key = ("esearch", db, term, retmax)
        cached = self._cache.get(key)
        if cached is None:
            record = self._eutil("esearch", {"db": db, "term": term, "retmax": retmax})
            cached = self._cache[key] = [str(uid) for uid in record.get("IdList", [])]
        return list(cached)

    def esummary(self, db: str, uid: str) -> dict[str, Any]:
        """Fetch the document summary for a single UID.

        Handles both esummary response shapes: the classic ``DocSum`` list used
        by ``gds``/``sra`` and the v2.0 ``DocumentSummarySet`` used by databases
        such as ``bioproject``.

        Parameters
        ----------
        db : str
            Entrez database (e.g. ``"gds"``, ``"sra"``, or ``"bioproject"``).
        uid : str
            The UID whose summary to fetch.

        Returns
        -------
        dict
            The document summary as a plain dictionary. Cached per client.

        Raises
        ------
        EntrezError
            If NCBI returns no summary for the UID.
        """
        key = ("esummary", db, str(uid))
        cached = self._cache.get(key)
        if cached is None:
            record = self._eutil("esummary", {"db": db, "id": str(uid)})
            docsums = _docsums(record)
            if not docsums:
                raise EntrezError(f"empty esummary for {db} uid {uid!r}")
            cached = self._cache[key] = dict(docsums[0])
        return cached

    def esummary_many(self, db: str, uids: list[str]) -> list[dict[str, Any]]:
        """Fetch document summaries for several UIDs in a single request.

        Collapses what would otherwise be one ESummary per UID into one batched
        call (NCBI accepts a comma-joined id list), chunked to keep the request
        URL within limits. Use this instead of looping over :meth:`esummary`.

        Parameters
        ----------
        db : str
            Entrez database (e.g. ``"gds"``, ``"sra"``, or ``"bioproject"``).
        uids : list of str
            The UIDs whose summaries to fetch. An empty list issues no request.

        Returns
        -------
        list of dict
            One document summary per UID, in NCBI's order. The mappings may carry
            NCBI's ``Id``/``uid`` so callers can recover each summary's UID via
            :func:`docsum_uid`. Cached per client by the exact UID list.
        """
        ids = [str(uid) for uid in uids]
        if not ids:
            return []
        key = ("esummary_many", db, tuple(ids))
        cached = self._cache.get(key)
        if cached is None:
            docsums: list[Any] = []
            for chunk in _chunked(ids, _ESUMMARY_CHUNK):
                record = self._eutil("esummary", {"db": db, "id": ",".join(chunk)})
                docsums.extend(_docsums(record))
            cached = self._cache[key] = docsums
        return list(cached)

    def elink(self, dbfrom: str, db: str, uid: str) -> list[str]:
        """Return UIDs in ``db`` linked from ``uid`` in ``dbfrom``.

        Parameters
        ----------
        dbfrom : str
            Source database (e.g. ``"gds"``).
        db : str
            Target database (e.g. ``"sra"``).
        uid : str
            The source UID.

        Returns
        -------
        list of str
            Linked UIDs in the target database (empty if there are none). Cached
            per client.
        """
        key = ("elink", dbfrom, db, str(uid))
        cached = self._cache.get(key)
        if cached is None:
            record = self._eutil("elink", {"dbfrom": dbfrom, "db": db, "id": str(uid)})
            linked: list[str] = []
            for linkset in record:
                for linksetdb in linkset.get("LinkSetDb", []):
                    linked.extend(str(link["Id"]) for link in linksetdb.get("Link", []))
            cached = self._cache[key] = linked
        return list(cached)

    def elink_many(self, dbfrom: str, db: str, uids: list[str]) -> dict[str, list[str]]:
        """Map each of several UIDs to the UIDs it links to, one request per chunk.

        Parameters
        ----------
        dbfrom : str
            Source database (e.g. ``"gds"``).
        db : str
            Target database (e.g. ``"sra"``).
        uids : list of str
            The source UIDs. An empty list issues no request.

        Returns
        -------
        dict of str to list of str
            Each distinct source UID mapped to its linked UIDs (empty if none).
        """
        wanted = list(dict.fromkeys(str(uid) for uid in uids))
        out: dict[str, list[str]] = {uid: [] for uid in wanted}
        for start in range(0, len(wanted), _ESUMMARY_CHUNK):
            chunk = wanted[start : start + _ESUMMARY_CHUNK]
            # A list ``id`` is sent as repeated parameters, so NCBI answers one set per UID.
            record = self._eutil("elink", {"dbfrom": dbfrom, "db": db, "id": chunk})
            for linkset in record:
                source = str((linkset.get("IdList") or [""])[0])
                for linksetdb in linkset.get("LinkSetDb", []):
                    out.setdefault(source, []).extend(
                        str(link["Id"]) for link in linksetdb.get("Link", [])
                    )
        return out

    def efetch(
        self,
        db: str,
        ids: list[str],
        *,
        rettype: str,
        retmode: str = "text",
        refresh: bool = False,
    ) -> str:
        """Fetch raw records via EFetch and return the response as text.

        Unlike the other helpers this returns the response **unparsed** (e.g. the
        SRA ``runinfo`` CSV), because EFetch serves formats Biopython does not
        parse into Python structures.

        Parameters
        ----------
        db : str
            Entrez database (e.g. ``"sra"``).
        ids : list of str
            The UIDs to fetch. An empty list issues no request.
        rettype : str
            EFetch return type (e.g. ``"runinfo"``).
        retmode : str
            EFetch return mode (default ``"text"``).
        refresh : bool
            Skip the cached response and request again (the new response is cached).

        Returns
        -------
        str
            The raw response body (empty string for an empty ``ids``). Cached per
            client by the exact request.
        """
        uids = [str(uid) for uid in ids]
        if not uids:
            return ""
        key = ("efetch", db, tuple(uids), rettype, retmode)
        cached = None if refresh else self._cache.get(key)
        if cached is None:
            params = {"db": db, "id": ",".join(uids), "rettype": rettype, "retmode": retmode}
            cached = self._cache[key] = self._eutil("efetch", params, _read_text)
        return cached

    def efetch_records(
        self,
        db: str,
        ids: list[str],
        *,
        rettype: str,
        retmode: str = "xml",
        split: Callable[[str], Mapping[str, str]],
    ) -> FetchedRecords:
        """Fetch one batch of records, re-asking once for the ids the reply left out.

        NCBI can leave records out of a batch with no error; those ids are asked for again
        together. An "ID list is empty" answer marks the ids it covers missing and is not
        retried. A failed re-ask never drops what the batch already returned. Nothing is
        cached.

        Parameters
        ----------
        db : str
            Entrez database (e.g. ``"sra"``).
        ids : list of str
            The ids to fetch in one request.
        rettype : str
            EFetch return type (e.g. ``"full"``).
        retmode : str
            EFetch return mode (default ``"xml"``).
        split : callable
            Maps a reply's text to ``{id: record text}``, keyed as ``ids`` are.

        Returns
        -------
        FetchedRecords
            The records got, the ids NCBI has none for, and the ids whose re-ask failed.

        Raises
        ------
        EntrezError
            If the first request fails for a reason other than "ID list is empty".
        """
        out = FetchedRecords()
        todo = list(dict.fromkeys(str(i) for i in ids))
        for ask in range(2):
            params = {"db": db, "id": ",".join(todo), "rettype": rettype, "retmode": retmode}
            try:
                reply = self._eutil("efetch", params, _read_text)
            except EntrezError as err:
                if _ids_empty(err.__cause__ or err):
                    out.missing = todo
                elif ask == 0:
                    raise
                else:
                    out.failed = dict.fromkeys(todo, str(err))
                return out
            asked = set(todo)
            out.records |= {k: v for k, v in split(reply).items() if k in asked}
            todo = [i for i in todo if i not in out.records]
            if not todo:
                break
        out.missing = todo
        return out

    def fetch_text(self, url: str) -> str:
        """GET an NCBI web page that E-utilities does not serve and return its text.

        Parameters
        ----------
        url : str
            The page URL.

        Returns
        -------
        str
            The response body. Cached per client by URL.

        Raises
        ------
        PageNotFound
            If the server answers 404.
        EntrezError
            If the request fails after the transient-failure retries.
        """
        key = ("fetch_text", url)
        cached = self._cache.get(key)
        if cached is None:
            cached = self._cache[key] = self._get(url, _read_text)
        return cached

    def download(self, url: str, dest: Path) -> int:
        """Stream a file from the GEO site to ``dest`` as it arrives, never whole in memory.

        The bytes are written as served (a ``.gz`` stays compressed) to a temporary file
        beside ``dest`` and renamed into place once complete.

        Parameters
        ----------
        url : str
            The file URL.
        dest : Path
            Where to write it.

        Returns
        -------
        int
            Bytes written.

        Raises
        ------
        PageNotFound
            If the server answers 404.
        EntrezError
            If the request fails after the transient-failure retries.
        """
        part = dest.with_name(dest.name + ".part")

        def write(response: Response) -> int:
            size = 0
            with part.open("wb") as fh:
                while chunk := response.read(_CHUNK_BYTES):
                    fh.write(chunk)
                    size += len(chunk)
            return size

        try:
            size = self._get(url, write)
        except BaseException:
            part.unlink(missing_ok=True)
            raise
        part.replace(dest)
        return size

    def stream(
        self, url: str, consume: Callable[[Iterator[bytes]], _T], *, tries: int = _MAX_TRIES
    ) -> _T:
        """GET a file from the GEO site and hand its lines to ``consume`` as they arrive.

        A gzip body is decompressed on the way. The body is read in chunks, so a large file
        is never whole in memory unless ``consume`` keeps it. Nothing is cached.

        Parameters
        ----------
        url : str
            The file URL.
        consume : callable
            Takes an iterator over the body's lines, as bytes with their line endings, and
            returns the result. It runs again from the start on each retry.
        tries : int
            Tries before giving up; ``1`` leaves retries and backoff to the caller.

        Returns
        -------
        object
            What ``consume`` returned.

        Raises
        ------
        PageNotFound
            If the server answers 404.
        EntrezError
            If the request fails ``tries`` times; the last failure is its ``__cause__``.
        """

        def read(response: Response) -> _T:
            body = io.BufferedReader(_RawBody(response), _CHUNK_BYTES)
            lines: io.BufferedIOBase = body
            if body.peek(2)[:2] == b"\x1f\x8b":
                lines = gzip.GzipFile(fileobj=body)
            result = consume(iter(lines))
            while body.read(_CHUNK_BYTES):  # drain, so the connection can be reused
                pass
            return result

        return self._get(url, read, tries=tries)

    def _get(self, url: str, read: Callable[[Response], Any], *, tries: int = _MAX_TRIES) -> Any:
        """GET a page outside E-utilities; a 404 raises :class:`PageNotFound`."""
        try:
            return self._send("GET", url, None, _GEO_TIMEOUT, read, limiter=None, tries=tries)
        except EntrezError as err:
            cause = err.__cause__
            if isinstance(cause, urllib.error.HTTPError) and cause.code == 404:
                raise PageNotFound(f"no page at {url}") from cause
            raise

    def _eutil(
        self,
        util: str,
        params: Mapping[str, Any],
        read: Callable[[Response], Any] = _read_xml,
    ) -> Any:
        """POST one E-utilities call, paced by the shared limiter."""
        body = urllib.parse.urlencode({**params, **self._identity}, doseq=True).encode()
        url = f"{_EUTILS}/{util}.fcgi"
        return self._send("POST", url, body, _EUTILS_TIMEOUT, read, limiter=self._limiter)

    def _send(
        self,
        method: str,
        url: str,
        body: bytes | None,
        timeout: float,
        read: Callable[[Response], Any],
        *,
        limiter: RateLimiter | None,
        tries: int = _MAX_TRIES,
    ) -> Any:
        """Send a request and read its reply, retrying transient failures with backoff.

        The send and the read are one retryable unit: NCBI most often fails by dropping the
        reply mid-stream. A 429 or 503 waits at least its ``Retry-After`` and halves the
        limiter's rate. Every failure ends as :class:`EntrezError`.
        """
        headers = {"User-Agent": _USER_AGENT}
        if body is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        refused = 0
        attempt = 0
        while True:
            try:
                pace = limiter.turn() if limiter else contextlib.nullcontext()
                with (
                    pace,
                    self._transport.open(
                        method, url, body=body, headers=headers, timeout=timeout
                    ) as response,
                ):
                    if response.status != 200:
                        raise _StatusError(
                            url, response.status, _headers(response), response.read()
                        )
                    return read(response)
            except Exception as err:
                attempt += 1
                wait = _backoff(attempt - 1)
                status = getattr(err, "code", None)
                if status in _THROTTLED:
                    wait = max(wait, _retry_after(err))
                    if limiter:
                        limiter.halve()
                retry = _is_transient(err)
                if limiter and isinstance(status, int) and status < 500 and not retry:
                    refused += 1
                    retry = status != 404 and not _ids_empty(err) and refused < _REFUSED_TRIES
                if not retry or attempt >= tries:
                    raise EntrezError(f"NCBI E-utilities request failed: {err}") from err
                time.sleep(wait)


class _RawBody(io.RawIOBase):
    """A response body as a raw stream, so it can be buffered and gunzipped."""

    def __init__(self, response: Response) -> None:
        self._response = response

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        data = self._response.read(min(len(buffer), _CHUNK_BYTES))
        buffer[: len(data)] = data
        return len(data)


def _headers(response: Response) -> Message:
    """Keep the one header the retry reads."""
    headers = Message()
    retry_after = response.getheader("Retry-After")
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return headers
