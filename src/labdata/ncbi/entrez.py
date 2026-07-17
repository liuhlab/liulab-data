"""A thin, mockable wrapper over Biopython's :mod:`Bio.Entrez`.

Every NCBI E-utilities call in liulab-data goes through :class:`EntrezClient`,
which sets the contact email / API key from
:func:`labdata.ncbi.config.load_credentials` and exposes small typed helpers
(``esearch``, ``esummary``, ``esummary_many``, ``elink``) that parse the XML
handle into plain Python structures, plus ``efetch`` for raw (e.g. CSV)
responses. Concentrating network access in one seam keeps the rest of the
package easy to unit-test by substituting a fake client.

Responses are memoized per client instance, so records that share a client never
repeat an identical request; ``esummary_many`` further collapses a list of UIDs
into one batched request.
"""

from __future__ import annotations

import http.client
import socket
import time
import urllib.error
from collections.abc import Callable
from typing import Any

from Bio import Entrez

from labdata.exceptions import EntrezError
from labdata.ncbi.config import NcbiCredentials, load_credentials

# Bio.Entrez ships no type stubs and exposes mutable module-level globals; alias
# it as Any so the type checker doesn't fight Biopython's dynamic API.
_entrez: Any = Entrez


#: Maximum UIDs per batched esummary request (keeps the GET URL within limits).
_ESUMMARY_CHUNK = 200

#: A request is tried this many times before giving up (one initial try plus
#: retries). NCBI drops a response mid-stream far more often than it refuses one,
#: and a single dropped efetch is not a reason to fail a whole records fetch.
_MAX_TRIES = 4

#: Base seconds for the exponential backoff between retries (1s, 2s, 4s). Even with
#: an API key (10 req/s) NCBI occasionally closes a connection early, so backing off
#: and trying again is the difference between a transient blip and a failed fetch.
_RETRY_BASE_DELAY = 1.0

#: Connection-level failures worth retrying — the request reached NCBI and the
#: response, not the query, is what went wrong.
_TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    http.client.IncompleteRead,
    http.client.RemoteDisconnected,
    ConnectionError,
    TimeoutError,
    socket.timeout,
    EOFError,
)

#: Substrings marking a transient drop when it reaches us as a bare ``RuntimeError``.
#: Bio.Entrez raises ``RuntimeError`` both for NCBI's own logical errors (permanent —
#: "ID list is empty") and, via the XML parser, when the connection closes mid-parse
#: ("Response ended prematurely" / "Read failed: EOF"). Matching the message is how we
#: retry the latter without also hammering NCBI over a query it will never accept.
_TRANSIENT_MESSAGES: tuple[str, ...] = (
    "ended prematurely",
    "response ended",
    "read failed",
    "unexpectedly closed",
    "connection reset",
    "timed out",
    "incompleteread",
)


def _is_transient(err: BaseException) -> bool:
    """Decide whether a failure is a retryable drop or the query's own permanent fault."""
    if isinstance(err, urllib.error.HTTPError):
        # 429 (rate limited) and 5xx (server) may clear on a retry; 4xx is our request.
        return err.code == 429 or err.code >= 500
    if isinstance(err, urllib.error.URLError):
        return True  # DNS / refused / reset at the socket layer
    if isinstance(err, _TRANSIENT_EXCEPTIONS):
        return True
    return any(marker in str(err).lower() for marker in _TRANSIENT_MESSAGES)


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


def _docsum_uid(docsum: Any) -> str | None:
    """Return the NCBI UID of a single document summary, if present.

    Classic ``DocSum`` records (``gds``/``sra``) carry the UID under the ``Id``
    key; v2.0 ``DocumentSummary`` records (``bioproject``) carry it as the XML
    ``uid`` attribute, which Biopython exposes via ``.attributes``.
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
    """Authenticated entry point to NCBI E-utilities.

    Parameters
    ----------
    credentials : NcbiCredentials or None
        Credentials to use. When ``None`` (default) they are resolved via
        :func:`labdata.ncbi.config.load_credentials`.
    allow_prompt : bool
        Forwarded to :func:`load_credentials` when ``credentials`` is ``None``.
    """

    def __init__(
        self,
        credentials: NcbiCredentials | None = None,
        *,
        allow_prompt: bool = True,
    ) -> None:
        self.credentials = credentials or load_credentials(allow_prompt=allow_prompt)
        _entrez.email = self.credentials.email
        if self.credentials.api_key:
            _entrez.api_key = self.credentials.api_key
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
            record = self._request(
                lambda: _entrez.esearch(db=db, term=term, retmax=retmax), _entrez.read
            )
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
            record = self._request(lambda: _entrez.esummary(db=db, id=str(uid)), _entrez.read)
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
            :func:`_docsum_uid`. Cached per client by the exact UID list.
        """
        ids = [str(uid) for uid in uids]
        if not ids:
            return []
        key = ("esummary_many", db, tuple(ids))
        cached = self._cache.get(key)
        if cached is None:
            docsums: list[Any] = []
            for chunk in _chunked(ids, _ESUMMARY_CHUNK):
                # Bind the chunk's id string by value: the lambda outlives the loop iteration,
                # and a plain default (not a call) keeps both B023 and B008 happy.
                chunk_ids = ",".join(chunk)
                record = self._request(
                    lambda cid=chunk_ids: _entrez.esummary(db=db, id=cid), _entrez.read
                )
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
            record = self._request(
                lambda: _entrez.elink(dbfrom=dbfrom, db=db, id=str(uid)), _entrez.read
            )
            linked: list[str] = []
            for linkset in record:
                for linksetdb in linkset.get("LinkSetDb", []):
                    linked.extend(str(link["Id"]) for link in linksetdb.get("Link", []))
            cached = self._cache[key] = linked
        return list(cached)

    def efetch(
        self,
        db: str,
        ids: list[str],
        *,
        rettype: str,
        retmode: str = "text",
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
        cached = self._cache.get(key)
        if cached is None:
            cached = self._cache[key] = self._request(
                lambda: _entrez.efetch(db=db, id=",".join(uids), rettype=rettype, retmode=retmode),
                self._parse_text,
            )
        return cached

    def _request(self, open_handle: Callable[[], Any], parse: Callable[[Any], Any]) -> Any:
        """Open an E-utilities handle and parse it, retrying transient failures with backoff.

        The open and the parse are one retryable unit on purpose: NCBI most often fails
        by accepting the request and then dropping the *response* mid-stream, so the error
        surfaces inside ``parse`` (``Entrez.read`` / ``handle.read``), not at open time.
        Retrying only the open would replay the same drop. A permanent failure (a 4xx, an
        empty id list) is not retried — :func:`_is_transient` tells them apart — and every
        failure still normalizes to :class:`EntrezError`, so callers are unchanged.
        """
        last: BaseException | None = None
        for attempt in range(_MAX_TRIES):
            try:
                with open_handle() as handle:
                    return parse(handle)
            except Exception as err:
                last = err
                if _is_transient(err) and attempt + 1 < _MAX_TRIES:
                    time.sleep(_RETRY_BASE_DELAY * 2**attempt)
                    continue
                raise EntrezError(f"NCBI E-utilities request failed: {err}") from err
        # The loop returns a value or raises; this line only satisfies the type checker.
        raise EntrezError(f"NCBI E-utilities request failed: {last}") from last

    @staticmethod
    def _parse_text(handle: Any) -> str:
        """Read a raw E-utilities handle to text (EFetch serves formats Biopython won't parse)."""
        data = handle.read()
        return data.decode() if isinstance(data, bytes) else str(data)
