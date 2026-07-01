r"""Shared foundation for the GEO/SRA object model.

Holds the pieces every record module needs but that carry no dependency on the
concrete record classes:

* the loosely-structured-XML parsing helpers (SRA ``ExpXml``/``Runs`` fragments
  come back as escaped text, so attributes are pulled out by regex), plus a few
  GEO id/bucket helpers; and
* :class:`_Record`, the accessioned-record base (validation, client seam, identity).

Keeping these here lets :mod:`labdata.geo.geo_records`, :mod:`labdata.geo.sra_records`,
and :mod:`labdata.geo.bio_project_records` import a common base without importing
one another — the concrete classes reference each other mutually, so a shared,
dependency-free base module is what breaks the would-be import cycle.
"""

from __future__ import annotations

import re
from typing import Any, ClassVar, Self

from labdata.exceptions import AccessionError
from labdata.ncbi.entrez import EntrezClient
from labdata.ncbi.sdl import SdlClient

# --------------------------------------------------------------------------- #
# small parsing helpers (SRA ExpXml/Runs are loosely structured XML fragments
# returned as escaped text, so we extract attributes by regex rather than re-parse)
# --------------------------------------------------------------------------- #


def _attr(fragment: str, element: str, attribute: str) -> str:
    """Return the first ``element``'s ``attribute`` value in an XML ``fragment``."""
    match = re.search(rf'<{element}\b[^>]*?\b{attribute}="([^"]*)"', fragment)
    return match.group(1) if match else ""


def _attrs_all(fragment: str, element: str, attribute: str) -> list[str]:
    """Return every ``element``'s ``attribute`` value in an XML ``fragment``."""
    return re.findall(rf'<{element}\b[^>]*?\b{attribute}="([^"]*)"', fragment)


def _tag_text(fragment: str, tag: str) -> str:
    """Return the text content of the first ``<tag>...</tag>`` in ``fragment``."""
    match = re.search(rf"<{tag}\b[^>]*>(.*?)</{tag}>", fragment, re.S)
    return match.group(1) if match else ""


def _int_attr(tag: str, attribute: str) -> int | None:
    """Return an integer ``attribute`` from a single element ``tag``, or ``None``."""
    match = re.search(rf'\b{attribute}="(\d+)"', tag)
    return int(match.group(1)) if match else None


def _read_lengths(run_block: str) -> list[int]:
    """Return the average length of each read in a ``<RUN>`` block, in read order.

    Reads the per-read ``<Read index=… average=…/>`` rows of the run's
    ``<Statistics>`` element (the empirical read lengths SRA reports), rounding
    each average to the nearest base and ordering by ``index``. Returns an empty
    list when the block carries no read statistics.
    """
    lengths: list[tuple[int, int]] = []
    for tag in re.findall(r"<Read\b[^>]*?/?>", run_block):
        index = _int_attr(tag, "index")
        average = re.search(r'\baverage="([\d.]+)"', tag)
        if index is not None and average is not None:
            lengths.append((index, round(float(average.group(1)))))
    return [length for _index, length in sorted(lengths)]


def _read_structure(lengths: list[int]) -> str:
    """Render read lengths as a ``{L1}+{L2}+…`` structure string (empty if none)."""
    return "+".join(str(length) for length in lengths)


def _run_blocks(full_xml: str) -> dict[str, str]:
    """Map each run accession in a full SRA XML to its ``<RUN …>`` element.

    Matches both a self-closing ``<RUN …/>`` and a ``<RUN …>…</RUN>`` body; the
    body form is tempered so a self-closing run never lets a match run on into the
    next run's element.
    """
    return {
        match.group(1): match.group(0)
        for match in re.finditer(
            r'<RUN\b[^>]*?\baccession="([SED]RR\d+)"[^>]*?(?:/>|>(?:(?!</?RUN\b).)*?</RUN>)',
            full_xml,
            re.S,
        )
    }


def _ftp_bucket(accession: str) -> str:
    """Return the GEO FTP bucket for ``accession`` (e.g. ``GSE131907`` -> ``GSE131nnn``)."""
    match = re.match(r"^([A-Za-z]+)(\d+)$", accession)
    if match is None:
        return accession
    prefix, digits = match.group(1), match.group(2)
    head = digits[:-3] if len(digits) > 3 else ""
    return f"{prefix}{head}nnn"


def _as_gpl(token: str) -> str:
    """Normalize a platform token to ``GPL…`` form."""
    return token.upper() if token.upper().startswith("GPL") else f"GPL{token}"


def _split_ids(raw: str) -> list[str]:
    """Split a summary field holding several ids on whitespace/comma/semicolon."""
    return [tok for tok in re.split(r"[;,\s]+", raw) if tok]


# --------------------------------------------------------------------------- #
# base record
# --------------------------------------------------------------------------- #


class _Record:
    """Base for an accessioned NCBI record: validation, client, identity."""

    #: Pattern an accession must match (set by each concrete subclass).
    _ACCESSION_RE: ClassVar[re.Pattern[str]]
    #: Human-readable record kind, used in error messages.
    _KIND: ClassVar[str]

    def __init__(
        self,
        accession: str,
        *,
        client: EntrezClient | None = None,
        sdl_client: SdlClient | None = None,
    ) -> None:
        if not isinstance(accession, str):
            raise AccessionError(f"not a GEO/SRA {self._KIND} accession: {accession!r}")
        accession = accession.strip().upper()
        if not self._ACCESSION_RE.match(accession):
            raise AccessionError(f"not a GEO/SRA {self._KIND} accession: {accession!r}")
        self.accession = accession
        self._client = client
        self._sdl_client = sdl_client

    def __repr__(self) -> str:
        """Return an unambiguous representation."""
        return f"{type(self).__name__}({self.accession!r})"

    def __eq__(self, other: object) -> bool:
        """Return ``True`` when ``other`` is the same class with the same accession."""
        return isinstance(other, type(self)) and self.accession == other.accession

    def __hash__(self) -> int:
        """Hash by class and accession (consistent with :meth:`__eq__`)."""
        return hash((type(self).__name__, self.accession))

    @property
    def client(self) -> EntrezClient:
        """Return the Entrez client, constructing a default one on first use."""
        if self._client is None:
            self._client = EntrezClient()
        return self._client

    @property
    def sdl_client(self) -> SdlClient:
        """Return the SRA Data Locator client, constructing a default one on first use."""
        if self._sdl_client is None:
            self._sdl_client = SdlClient()
        return self._sdl_client

    def _seed(self, *, uid: str | None = None, summary: dict[str, Any] | None = None) -> Self:
        """Pre-populate the ``uid``/``_summary`` caches from an already-fetched response.

        Writes straight into the instance ``__dict__`` under the ``cached_property``
        names, so the seeded values are served without a request. Used when a
        batched lookup has already resolved a linked record's identity and summary.
        """
        if uid is not None:
            self.__dict__["uid"] = uid
        if summary is not None:
            self.__dict__["_summary"] = summary
        return self
