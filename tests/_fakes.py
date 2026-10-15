"""Test doubles for the network seam.

``FakeEntrezClient`` subclasses :class:`~labdata.ncbi.entrez.EntrezClient` (so it
is accepted wherever a real client is expected) but bypasses credential
resolution and serves canned responses while recording calls.
"""

from __future__ import annotations

from typing import Any

from labdata.ncbi.entrez import EntrezClient
from labdata.ncbi.sdl import RemoteFile, SdlClient


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
    packages : dict[str, str] or None
        Maps an ``sra`` id (UID or accession) to its ``EXPERIMENT_PACKAGE`` text. An
        ``efetch db=sra rettype=full`` naming any of these ids returns a package set of
        exactly the requested ids it knows.
    drops : dict[str, int] or None
        Maps an id in ``packages`` to how many package fetches leave it out, as NCBI
        sometimes does with no error.
    pages : dict[str, str] or None
        Maps a URL to the text ``fetch_text`` should return (empty when unmapped).
    """

    def __init__(
        self,
        *,
        esearch: dict[str, list[str]] | None = None,
        esummary: dict[tuple[str, str], dict[str, Any]] | None = None,
        elink: dict[tuple[str, str, str], list[str]] | None = None,
        efetch: dict[tuple[str, str], str] | None = None,
        pages: dict[str, str] | None = None,
        packages: dict[str, str] | None = None,
        drops: dict[str, int] | None = None,
    ) -> None:
        # Intentionally does not call super().__init__ — no credentials, no network.
        self._esearch = esearch or {}
        self._esummary = esummary or {}
        self._elink = elink or {}
        self._efetch = efetch or {}
        self._pages = pages or {}
        self._packages = packages or {}
        self._drops = dict(drops or {})
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

    def elink_many(self, dbfrom: str, db: str, uids: list[str]) -> dict[str, list[str]]:
        wanted = list(dict.fromkeys(str(uid) for uid in uids))
        self.calls.append(("elink_many", (dbfrom, db, tuple(wanted))))
        return {uid: self._elink.get((dbfrom, db, uid), []) for uid in wanted}

    def efetch(
        self,
        db: str,
        ids: list[str],
        *,
        rettype: str,
        retmode: str = "text",
        refresh: bool = False,
    ) -> str:
        requested = [str(uid) for uid in ids]
        self.calls.append(("efetch", (db, tuple(requested), rettype)))
        if db == "sra" and rettype == "full" and any(i in self._packages for i in requested):
            return self._package_set(requested)
        return self._efetch.get((db, rettype), "")

    def _package_set(self, ids: list[str]) -> str:
        """Return a package set of the known ``ids``, leaving out those due a drop."""
        body = []
        for uid in ids:
            if self._drops.get(uid, 0) > 0:
                self._drops[uid] -= 1
            elif uid in self._packages:
                body.append(self._packages[uid])
        return "<EXPERIMENT_PACKAGE_SET>" + "".join(body) + "</EXPERIMENT_PACKAGE_SET>"

    def fetch_text(self, url: str) -> str:
        self.calls.append(("fetch_text", (url,)))
        return self._pages.get(url, "")

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


class ExplodingEntrezClient(FakeEntrezClient):
    """An Entrez client that fails the test on any request."""

    def __init__(self) -> None:
        super().__init__()

    def __getattribute__(self, name: str) -> Any:
        if name in {"esearch", "esummary", "esummary_many", "elink", "elink_many", "efetch", "fetch_text"}:
            raise AssertionError(f"unexpected network call: {name}")
        return super().__getattribute__(name)


class FakeSdlClient(SdlClient):
    """A recording stand-in for ``SdlClient`` seeded with canned file listings.

    Parameters
    ----------
    files : dict[str, list[RemoteFile]] or None
        Maps a run accession to the :class:`~labdata.ncbi.sdl.RemoteFile` list its
        ``retrieve`` should return.
    """

    def __init__(self, files: dict[str, list[RemoteFile]] | None = None) -> None:
        # Intentionally does not call super().__init__ — no network.
        self._files = files or {}
        self.calls: list[str] = []

    def retrieve(self, accession: str) -> list[RemoteFile]:
        self.calls.append(accession)
        return list(self._files.get(accession, []))
