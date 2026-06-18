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
    """

    def __init__(
        self,
        *,
        esearch: dict[str, list[str]] | None = None,
        esummary: dict[tuple[str, str], dict[str, Any]] | None = None,
        elink: dict[tuple[str, str, str], list[str]] | None = None,
    ) -> None:
        # Intentionally does not call super().__init__ — no credentials, no network.
        self._esearch = esearch or {}
        self._esummary = esummary or {}
        self._elink = elink or {}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def esearch(self, db: str, term: str, *, retmax: int = 20) -> list[str]:
        self.calls.append(("esearch", (db, term)))
        return self._esearch.get(term, [])

    def esummary(self, db: str, uid: str) -> dict[str, Any]:
        self.calls.append(("esummary", (db, uid)))
        return self._esummary[db, uid]

    def elink(self, dbfrom: str, db: str, uid: str) -> list[str]:
        self.calls.append(("elink", (dbfrom, db, uid)))
        return self._elink.get((dbfrom, db, uid), [])

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
