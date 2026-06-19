"""Test doubles for the network seam.

``FakeEntrezClient`` subclasses :class:`~labdata.ncbi.entrez.EntrezClient` (so it
is accepted wherever a real client is expected) but bypasses credential
resolution and serves canned responses while recording calls.
"""

from __future__ import annotations

from typing import Any

from labdata.ncbi.entrez import EntrezClient


class FakeEntrezClient(EntrezClient):
    """A recording stand-in for ``EntrezClient`` seeded with canned responses.

    Parameters
    ----------
    esearch : dict[str, list[str]] or None
        Maps an ESearch ``term`` to the UID list it should return.
    esummary : dict[tuple[str, str], dict] or None
        Maps ``(db, uid)`` to the document summary it should return.
    elink : dict[tuple[str, str, str], list[str]] or None
        Maps ``(dbfrom, db, uid)`` to the linked UID list it should return.
    efetch : dict[tuple[str, str], str] or None
        Maps ``(db, rettype)`` to the raw text EFetch should return.
    """

    def __init__(
        self,
        *,
        esearch: dict[str, list[str]] | None = None,
        esummary: dict[tuple[str, str], dict[str, Any]] | None = None,
        elink: dict[tuple[str, str, str], list[str]] | None = None,
        efetch: dict[tuple[str, str], str] | None = None,
    ) -> None:
        # Intentionally does not call super().__init__ — no credentials, no network.
        self._esearch = esearch or {}
        self._esummary = esummary or {}
        self._elink = elink or {}
        self._efetch = efetch or {}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def esearch(self, db: str, term: str, *, retmax: int = 20) -> list[str]:
        self.calls.append(("esearch", (db, term)))
        return self._esearch.get(term, [])

    def esummary(self, db: str, uid: str) -> dict[str, Any]:
        self.calls.append(("esummary", (db, uid)))
        return self._esummary[db, uid]

    def esummary_many(self, db: str, uids: list[str]) -> list[dict[str, Any]]:
        ids = [str(uid) for uid in uids]
        self.calls.append(("esummary_many", (db, tuple(ids))))
        return [self._esummary[db, uid] for uid in ids]

    def elink(self, dbfrom: str, db: str, uid: str) -> list[str]:
        self.calls.append(("elink", (dbfrom, db, uid)))
        return self._elink.get((dbfrom, db, uid), [])

    def efetch(self, db: str, ids: list[str], *, rettype: str, retmode: str = "text") -> str:
        self.calls.append(("efetch", (db, tuple(str(uid) for uid in ids), rettype)))
        return self._efetch.get((db, rettype), "")

    def count(self, method: str) -> int:
        """Return how many times ``method`` has been called."""
        return sum(1 for name, _ in self.calls if name == method)

    def but(
        self,
        *,
        esearch: dict[str, list[str]] | None = None,
        esummary: dict[tuple[str, str], dict[str, Any]] | None = None,
        elink: dict[tuple[str, str, str], list[str]] | None = None,
    ) -> FakeEntrezClient:
        """Replace whole canned-response maps in place and return self (for chaining)."""
        if esearch is not None:
            self._esearch = esearch
        if esummary is not None:
            self._esummary = esummary
        if elink is not None:
            self._elink = elink
        return self
