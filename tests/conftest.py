"""Shared fixtures for the test suite.

The whole package funnels NCBI access through ``labdata.ncbi.EntrezClient``, so
tests substitute :class:`FakeEntrezClient` — a recording stand-in seeded with
canned responses — and never touch the network.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest


class FakeEntrezClient:
    """A stand-in for ``EntrezClient`` that serves canned responses and records calls.

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


@pytest.fixture
def make_client() -> Callable[..., FakeEntrezClient]:
    """Return a factory that builds a :class:`FakeEntrezClient` from canned data."""

    def _make(**kwargs: Any) -> FakeEntrezClient:
        return FakeEntrezClient(**kwargs)

    return _make
