"""The vocabularies the store keeps: one typed entry each, and how a release is named.

A plain OBO Foundry vocabulary needs only its name and its own-term id prefixes; the address
and the version rule default to the OBO Foundry file and its ``data-version`` header.
"""

from __future__ import annotations

import datetime
import email.utils
import enum
import re
from dataclasses import dataclass

from labdata.exceptions import OntologyError

#: The address of an OBO Foundry vocabulary's OBO file.
OBO_FOUNDRY = "https://purl.obolibrary.org/obo/{name}.obo"

_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_VERSION_INFO = re.compile(r"<owl:versionInfo[^>]*>([^<]*)</owl:versionInfo>")


class ReaderKind(enum.StrEnum):
    """How a vocabulary's source files are read into terms."""

    OBO = "obo"
    #: Cellosaurus's OBO file, read line by line, and its list of deleted ids.
    CELLOSAURUS = "cellosaurus"
    #: NCBI Taxonomy's dump: scientific names, merged ids and deleted ids.
    TAXDUMP = "taxdump"


class VersionRule(enum.StrEnum):
    """Where a vocabulary states its release."""

    #: The ``data-version`` line of the OBO header.
    DATA_VERSION = "data-version"
    #: The ``owl:versionInfo`` of an OWL file's header, read at ``version_url``.
    OWL_VERSION_INFO = "owl:versionInfo"
    #: The ``Last-Modified`` date of the downloaded file, for a source rebuilt daily.
    FILE_DATE = "file-date"


@dataclass(frozen=True, slots=True)
class Vocabulary:
    """One vocabulary the store keeps.

    Parameters
    ----------
    name : str
        The vocabulary's name, such as ``fbdv``; its snapshots are ``<name>-<release>``.
    prefixes : tuple of str
        The id prefixes of its own terms, such as ``("FBdv:",)``; a reader keeps only these.
    url : str
        The download address; empty means the OBO Foundry file, :data:`OBO_FOUNDRY`.
    reader : ReaderKind
        How the downloaded file is read.
    version : VersionRule
        Where the release is read.
    version_url : str
        Where the release is read, when not at ``url``.
    also : tuple of str
        More files downloaded with ``url`` and kept beside it.
    searchable : bool
        ``False`` for a vocabulary that is looked up by id only and gets no search index.
    """

    name: str
    prefixes: tuple[str, ...]
    url: str = ""
    reader: ReaderKind = ReaderKind.OBO
    version: VersionRule = VersionRule.DATA_VERSION
    version_url: str = ""
    also: tuple[str, ...] = ()
    searchable: bool = True

    def __post_init__(self) -> None:
        """Fill in the OBO Foundry address when none is given."""
        if not self.url:
            object.__setattr__(self, "url", OBO_FOUNDRY.format(name=self.name))
        if not self.version_url:
            object.__setattr__(self, "version_url", self.url)

    @property
    def downloads(self) -> tuple[str, ...]:
        """Return every address the update downloads, ``url`` first."""
        return (self.url, *self.also)


#: The prefix of a taxonomy id; a lookup of a bare number means ``NCBITaxon:<number>``.
TAXON_PREFIX = "NCBITaxon:"

_CELLOSAURUS = "https://ftp.expasy.org/databases/cellosaurus/"
_DEV_STAGES = "https://purl.obolibrary.org/obo/{name}.owl"

#: Every vocabulary the update brings in.
VOCABULARIES: tuple[Vocabulary, ...] = (
    Vocabulary(
        "ncbitaxon",
        (TAXON_PREFIX,),
        url="https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz",
        reader=ReaderKind.TAXDUMP,
        version=VersionRule.FILE_DATE,
        searchable=False,
    ),
    Vocabulary("uberon", ("UBERON:",)),
    Vocabulary("wbbt", ("WBbt:",)),
    Vocabulary("cl", ("CL:",)),
    Vocabulary(
        "cellosaurus",
        ("CVCL_",),
        url=f"{_CELLOSAURUS}cellosaurus.obo",
        reader=ReaderKind.CELLOSAURUS,
        also=(f"{_CELLOSAURUS}cellosaurus_deleted_ACs.txt",),
    ),
    Vocabulary("mondo", ("MONDO:",)),
    Vocabulary("pato", ("PATO:",)),
    Vocabulary(
        "hsapdv",
        ("HsapDv:",),
        version=VersionRule.OWL_VERSION_INFO,
        version_url=_DEV_STAGES.format(name="hsapdv"),
    ),
    Vocabulary(
        "mmusdv",
        ("MmusDv:",),
        version=VersionRule.OWL_VERSION_INFO,
        version_url=_DEV_STAGES.format(name="mmusdv"),
    ),
    Vocabulary("wbls", ("WBls:",)),
    Vocabulary("chebi", ("CHEBI:",)),
    Vocabulary("fbdv", ("FBdv:",)),
)


def release_of(version: str) -> str:
    """Return the release a version string names: its ``YYYY-MM-DD`` date, or else itself.

    Raises
    ------
    OntologyError
        If the release would not make a plain folder name.

    Examples
    --------
    >>> release_of("fbdv/releases/2026-11-02")
    '2026-11-02'
    >>> release_of("257")
    '257'
    """
    found = _DATE.search(version)
    release = found.group(0) if found else version.strip()
    if not _SAFE.fullmatch(release):
        raise OntologyError(f"version {version!r} names no usable release")
    return release


def owl_version(head: bytes) -> str:
    """Return the ``owl:versionInfo`` of an OWL (RDF/XML) file, given its first bytes.

    Raises
    ------
    OntologyError
        If the bytes hold no ``owl:versionInfo``.
    """
    found = _VERSION_INFO.search(head.decode("utf-8", errors="replace"))
    if found is None:
        raise OntologyError("the OWL header states no owl:versionInfo")
    return found.group(1).strip()


def file_date(last_modified: str | None) -> str:
    """Return the UTC date, ``YYYY-MM-DD``, of an HTTP ``Last-Modified`` header.

    Raises
    ------
    OntologyError
        If the header is missing or is no date.

    Examples
    --------
    >>> file_date("Sat, 21 Nov 2026 16:29:20 GMT")
    '2026-11-21'
    """
    try:
        stamp = email.utils.parsedate_to_datetime(last_modified or "")
    except (TypeError, ValueError):
        raise OntologyError(f"Last-Modified {last_modified!r} is no date") from None
    if stamp.tzinfo is None:  # "-0000" means UTC with no zone stated
        stamp = stamp.replace(tzinfo=datetime.UTC)
    return stamp.astimezone(datetime.UTC).date().isoformat()
