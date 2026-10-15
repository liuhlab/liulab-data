"""Clients that refuse network use, held by store-built ``Experiment`` and ``Run`` objects."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, NoReturn

from labdata.exceptions import LabdataError
from labdata.ncbi.entrez import EntrezClient, FetchedRecords
from labdata.ncbi.sdl import RemoteFile, SdlClient


def _refuse(what: str) -> NoReturn:
    raise LabdataError(
        f"a store-built Experiment or Run makes no network call ({what}); "
        "use labdata.experiments_for for live data"
    )


class OfflineEntrezClient(EntrezClient):
    """An Entrez client that raises on any use."""

    def __init__(self) -> None:
        # No credentials and no cache: nothing here may reach NCBI.
        self._cache = {}

    def esearch(self, db: str, term: str, *, retmax: int = 20) -> list[str]:
        _refuse("esearch")

    def esummary(self, db: str, uid: str) -> dict[str, Any]:
        _refuse("esummary")

    def esummary_many(self, db: str, uids: list[str]) -> list[dict[str, Any]]:
        _refuse("esummary")

    def elink(self, dbfrom: str, db: str, uid: str) -> list[str]:
        _refuse("elink")

    def elink_many(self, dbfrom: str, db: str, uids: list[str]) -> dict[str, list[str]]:
        _refuse("elink")

    def efetch(
        self,
        db: str,
        ids: list[str],
        *,
        rettype: str,
        retmode: str = "text",
        refresh: bool = False,
    ) -> str:
        _refuse("efetch")

    def fetch_text(self, url: str) -> str:
        _refuse("fetch_text")

    def efetch_records(
        self,
        db: str,
        ids: list[str],
        *,
        rettype: str,
        retmode: str = "xml",
        split: Callable[[str], Mapping[str, str]],
    ) -> FetchedRecords:
        _refuse("efetch")

    def download(self, url: str, dest: Path) -> int:
        _refuse("download")

    def _send(self, *args: Any, **kwargs: Any) -> Any:
        _refuse("request")


class OfflineSdlClient(SdlClient):
    """An SRA Data Locator client that raises on any use."""

    def __init__(self) -> None:
        pass

    def retrieve(self, accession: str) -> list[RemoteFile]:
        _refuse("SRA Data Locator")
