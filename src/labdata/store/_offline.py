"""Clients that make no network call, held by store-built ``Experiment`` and ``Run`` objects."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, NoReturn

from labdata.exceptions import LabdataError
from labdata.ncbi.entrez import EntrezClient, FetchedRecords
from labdata.ncbi.sdl import FileLocation, RemoteFile, SdlClient


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

    def eutil_text(self, util: str, params: Mapping[str, str]) -> str:
        _refuse(util)

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


class StoredSdlClient(SdlClient):
    """An SRA Data Locator stand-in that lists a run's files from its stored package XML.

    It returns the ``<SRAFiles>`` entries as the XML gives them, with only the URLs the
    XML carries; it never asks SDL, so signed and cloud-only locations are absent.

    Parameters
    ----------
    packages : mapping of str to str
        Stored ``EXPERIMENT_PACKAGE`` text, keyed by experiment accession.
    """

    def __init__(self, packages: Mapping[str, str]) -> None:
        self._packages = packages
        self._cache = {}
        self._experiment_of: dict[str, str] | None = None

    def retrieve(self, accession: str) -> list[RemoteFile]:
        """Return the files the stored package lists for run ``accession``."""
        cached = self._cache.get(accession)
        if cached is None:
            cached = self._cache[accession] = self._read(accession)
        return list(cached)

    def _read(self, accession: str) -> list[RemoteFile]:
        if self._experiment_of is None:
            self._experiment_of = {
                run: experiment
                for experiment, text in self._packages.items()
                for run in _RUN_ACC.findall(text)
            }
        experiment = self._experiment_of.get(accession)
        if experiment is None:
            return []
        package = ET.fromstring(self._packages[experiment])
        for run in package.iterfind("RUN_SET/RUN"):
            if run.get("accession") == accession:
                return [_stored_file(f, accession) for f in run.iterfind("SRAFiles/SRAFile")]
        return []


_RUN_ACC = re.compile(r'<RUN\b[^>]*?\baccession="([^"]+)"')


def _stored_file(element: ET.Element, accession: str) -> RemoteFile:
    locations = [
        FileLocation(service=a.get("org", ""), region=None, link=a.get("url", ""))
        for a in element.iterfind("Alternatives")
        if a.get("url")
    ]
    url = element.get("url")
    if url and url not in {loc.link for loc in locations}:
        locations.insert(0, FileLocation(service="", region=None, link=url))
    size = element.get("size", "")
    return RemoteFile(
        name=element.get("filename", ""),
        type=element.get("semantic_name", ""),
        size=int(size) if size.isdigit() else None,
        md5=element.get("md5"),
        modification_date=element.get("date"),
        accession=accession,
        locations=tuple(locations),
        supertype=element.get("supertype"),
    )
