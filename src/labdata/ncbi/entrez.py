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

from typing import Any

from Bio import Entrez

from labdata.exceptions import EntrezError
from labdata.ncbi.config import NcbiCredentials, load_credentials

# Bio.Entrez ships no type stubs and exposes mutable module-level globals; alias
# it as Any so the type checker doesn't fight Biopython's dynamic API.
_entrez: Any = Entrez


#: Maximum UIDs per batched esummary request (keeps the GET URL within limits).
_ESUMMARY_CHUNK = 200


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
            record = self._read(_entrez.esearch(db=db, term=term, retmax=retmax))
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
            record = self._read(_entrez.esummary(db=db, id=str(uid)))
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
                record = self._read(_entrez.esummary(db=db, id=",".join(chunk)))
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
            record = self._read(_entrez.elink(dbfrom=dbfrom, db=db, id=str(uid)))
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
            cached = self._cache[key] = self._read_text(
                _entrez.efetch(db=db, id=",".join(uids), rettype=rettype, retmode=retmode)
            )
        return cached

    @staticmethod
    def _read(handle: Any) -> Any:
        """Parse an E-utilities handle, normalizing any failure to EntrezError."""
        try:
            with handle:
                return _entrez.read(handle)
        except Exception as err:
            raise EntrezError(f"NCBI E-utilities request failed: {err}") from err

    @staticmethod
    def _read_text(handle: Any) -> str:
        """Read a raw E-utilities handle to text, normalizing failures to EntrezError."""
        try:
            with handle:
                data = handle.read()
        except Exception as err:
            raise EntrezError(f"NCBI E-utilities request failed: {err}") from err
        return data.decode() if isinstance(data, bytes) else str(data)
