"""A thin, mockable client for NCBI's SRA Data Locator (SDL).

The SRA Run Browser's "Data access" panel — both the normalized **SRA archive**
files and the **original format** files the submitter uploaded — is served by the
SDL service rather than by E-utilities. :class:`SdlClient` wraps the public
``sdl/2/retrieve`` endpoint and returns the file listing as plain
:class:`RemoteFile` objects, so the rest of the package can discover (for example)
the original 10X Genomics BAM behind a run whose ``.sra`` only stored one read.

Concentrating this network access in one seam keeps callers easy to unit-test by
substituting a fake client, mirroring :class:`labdata.ncbi.entrez.EntrezClient`.
Responses are memoized per client instance, keyed by accession.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from labdata.exceptions import SdlError

#: Base URL of the SDL "retrieve" endpoint (version 2).
SDL_RETRIEVE_URL = "https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve"

#: ``type`` values SDL uses for the normalized SRA archive (everything else —
#: ``TenX``, ``bam``, ``fastq``, ``cram``, … — is an original/submitted file).
SRA_ARCHIVE_TYPES = frozenset({"sra", "sralite"})


@dataclass(frozen=True)
class FileLocation:
    """One place a :class:`RemoteFile` can be fetched from.

    Parameters
    ----------
    service : str
        The hosting service, e.g. ``"s3"``, ``"gs"``, or ``"ncbi"``.
    region : str or None
        The cloud region (e.g. ``"us-east-1"``), or ``None`` when unspecified.
    link : str
        The download URL or cloud URI for this location.
    """

    service: str
    region: str | None
    link: str


@dataclass(frozen=True)
class RemoteFile:
    """A single file SDL lists for a run, in any of its locations.

    Parameters
    ----------
    name : str
        The file name, e.g. ``"possorted_genome_bam_TC2_d15_1.bam"``.
    type : str
        SDL's file type, e.g. ``"sra"`` for the normalized archive or ``"TenX"``,
        ``"bam"``, ``"fastq"``, … for an original/submitted file.
    size : int or None
        File size in bytes, or ``None`` when unreported.
    md5 : str or None
        The file's MD5 checksum, or ``None`` when unreported.
    modification_date : str or None
        The file's modification timestamp (ISO 8601 string), or ``None``.
    accession : str
        The run accession this file belongs to.
    locations : tuple of FileLocation
        Every place the file can be fetched from, in SDL's order.
    """

    name: str
    type: str
    size: int | None
    md5: str | None
    modification_date: str | None
    accession: str
    locations: tuple[FileLocation, ...]

    @property
    def is_sra_archive(self) -> bool:
        """Whether this is a normalized SRA-archive file (vs. an original/submitted one)."""
        return self.type.lower() in SRA_ARCHIVE_TYPES

    @property
    def url(self) -> str:
        """The first location's download link, or ``""`` when there are none.

        SDL lists the anonymous, worldwide-egress HTTPS link first, so this is the
        simplest URL to fetch the file from.
        """
        return self.locations[0].link if self.locations else ""


class SdlClient:
    """Entry point to NCBI's SRA Data Locator.

    Parameters
    ----------
    base_url : str
        The ``retrieve`` endpoint to query (default :data:`SDL_RETRIEVE_URL`).
    timeout : float
        Per-request socket timeout, in seconds.
    """

    def __init__(self, base_url: str = SDL_RETRIEVE_URL, *, timeout: float = 30.0) -> None:
        self.base_url = base_url
        self.timeout = timeout
        # Per-instance response cache keyed by accession; redundant lookups across
        # records that share this client are served without a request.
        self._cache: dict[str, list[RemoteFile]] = {}

    def clear_cache(self) -> None:
        """Drop all cached responses (forcing fresh requests on the next call)."""
        self._cache.clear()

    def retrieve(self, accession: str) -> list[RemoteFile]:
        """Return every file SDL lists for ``accession``.

        Parameters
        ----------
        accession : str
            An SRA run accession, e.g. ``"SRR20172067"``.

        Returns
        -------
        list of RemoteFile
            Both the normalized SRA archive file(s) and any original/submitted
            files (e.g. a 10X ``possorted_genome_bam``). Cached per client.

        Raises
        ------
        SdlError
            If the request fails, the response is not valid JSON, or SDL reports a
            non-OK status for the accession (e.g. unknown or non-public run).
        """
        cached = self._cache.get(accession)
        if cached is None:
            payload = self._fetch(accession)
            cached = self._cache[accession] = _parse_files(payload, accession)
        return list(cached)

    def _fetch(self, accession: str) -> Any:
        """GET the SDL response for ``accession`` and parse it as JSON."""
        query = urllib.parse.urlencode({"acc": accession, "accept-alternate-locations": "yes"})
        request = urllib.request.Request(
            f"{self.base_url}?{query}",
            headers={"User-Agent": "liulab-data (labdata)"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read()
        except urllib.error.URLError as err:
            raise SdlError(f"SDL request failed for {accession!r}: {err}") from err
        try:
            return json.loads(body)
        except (ValueError, TypeError) as err:
            raise SdlError(f"SDL returned invalid JSON for {accession!r}: {err}") from err


def _parse_files(payload: Any, accession: str) -> list[RemoteFile]:
    """Build :class:`RemoteFile` objects from a parsed SDL ``retrieve`` payload."""
    results = payload.get("result", []) if isinstance(payload, dict) else []
    bundle = _select_bundle(results, accession)
    if bundle is None:
        raise SdlError(f"SDL returned no result for {accession!r}")
    status = bundle.get("status")
    if status not in (200, "200", None):
        msg = bundle.get("msg", "unknown error")
        raise SdlError(f"SDL status {status} for {accession!r}: {msg}")
    return [_build_file(entry, accession) for entry in bundle.get("files", [])]


def _select_bundle(results: Any, accession: str) -> dict[str, Any] | None:
    """Pick the bundle for ``accession`` (matching ``bundle``, else the first)."""
    bundles = [entry for entry in results if isinstance(entry, dict)]
    for entry in bundles:
        if entry.get("bundle") == accession:
            return entry
    return bundles[0] if bundles else None


def _build_file(entry: dict[str, Any], accession: str) -> RemoteFile:
    """Build one :class:`RemoteFile` from an SDL ``files`` entry."""
    locations = tuple(
        FileLocation(
            service=str(loc.get("service", "")),
            region=loc.get("region"),
            link=str(loc.get("link", "")),
        )
        for loc in entry.get("locations", [])
        if isinstance(loc, dict)
    )
    size = entry.get("size")
    return RemoteFile(
        name=str(entry.get("name", "")),
        type=str(entry.get("type", "")),
        size=int(size) if size is not None else None,
        md5=entry.get("md5"),
        modification_date=entry.get("modificationDate"),
        accession=str(entry.get("accession", accession)),
        locations=locations,
    )
