"""The store's record model: record types, relation names, ``Record`` and ``Link``.

Stdlib only: the one-record path imports neither pandas nor pyarrow.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from labdata.exceptions import AccessionError

#: Forward relation name to its reverse name. Each link is stored once, forward.
RELATIONS: dict[str, str] = {
    "SuperSeries of": "SubSeries of",
    "sample": "series",
    "platform": "platform of",
    "SRA": "GEO",
    "BioProject": "BioProject of",
    "BioSample": "BioSample of",
    "umbrella of": "under umbrella",
    "study": "experiment",
    "SRA sample": "SRA experiment",
    "run": "run of",
    "PubMed": "PubMed of",
    "Reanalyzed by": "Reanalysis of",
    "Named Annotation": "Named Annotation of",
}

#: Reverse relation name to its forward name.
INVERSE: dict[str, str] = {reverse: forward for forward, reverse in RELATIONS.items()}

#: A paper's link target: ``PMID:<n>``. A paper is not a record.
PAPER = "paper"
#: A GEO named annotation's link target, such as ``NA000049219.1``. It is not a record.
ANNOTATION = "named annotation"

_KINDS: tuple[tuple[re.Pattern[str], str], ...] = tuple(
    (re.compile(pattern), kind)
    for pattern, kind in (
        (r"GSE\d+", "GEO Series"),
        (r"GSM\d+", "GEO Sample"),
        (r"GPL\d+", "GEO Platform"),
        (r"[SED]RP\d+", "SRA study"),
        (r"[SED]RX\d+", "SRA experiment"),
        (r"[SED]RR\d+", "SRA run"),
        (r"[SED]RS\d+", "SRA sample"),
        (r"PRJ[A-Z]{2}\d+", "BioProject"),
        (r"SAM(?:N|EA|D)\d+", "BioSample"),
        (r"PMID:\d+", PAPER),
        (r"NA\d+(?:\.\d+)?", ANNOTATION),
    )
)


def kind_of(accession: str) -> str:
    """Return the record type an accession names.

    Parameters
    ----------
    accession : str
        A public accession such as ``"SRX24283130"``, or ``"PMID:<n>"``.

    Returns
    -------
    str
        One of the nine record types, ``"paper"`` for ``PMID:<n>``, or
        ``"named annotation"`` for a GEO named annotation.

    Raises
    ------
    AccessionError
        If the accession is malformed.
    """
    for pattern, kind in _KINDS:
        if pattern.fullmatch(accession):
            return kind
    raise AccessionError(f"not a resolvable accession: {accession!r}")


def forward_name(relation: str) -> str:
    """Return the forward name of a forward or reverse relation name.

    Raises
    ------
    ValueError
        If the name is not in the fixed relation list.
    """
    if relation in RELATIONS:
        return relation
    if relation in INVERSE:
        return INVERSE[relation]
    raise ValueError(f"unknown relation {relation!r}; known: {sorted(RELATIONS)}")


@dataclass(frozen=True, slots=True)
class Link:
    """One stated link, as read from the record it was looked up on.

    Parameters
    ----------
    relation : str
        The relation name as read from this end: forward, or its reverse name.
    other : str
        The accession at the far end, which the store may not hold.
    source : str
        Who stated it: ``SOFT``, ``SRA XML``, ``BioProject XML``, ``Entrez link`` or ``PubMed``.
    forward : bool
        Whether this end is the link's ``from``.
    """

    relation: str
    other: str
    source: str
    forward: bool


@dataclass(frozen=True, slots=True)
class Record:
    """One source record, as the store holds it.

    ``fields`` are the source's own (name, value) pairs in source order, repeats kept.
    Dates are ISO ``YYYY-MM-DD`` strings.
    """

    accession: str
    type: str
    status: str | None
    submitted: str | None
    updated: str | None
    published: str | None
    fields: tuple[tuple[str, str], ...]
    content_hash: str
    snapshot_date: str
    links: tuple[Link, ...]

    def field(self, name: str) -> list[str]:
        """Return every value of one field name, in source order."""
        return [value for key, value in self.fields if key == name]

    def follow(self, relation: str) -> list[str]:
        """Return the sorted accessions at the far end of one relation name.

        Raises
        ------
        ValueError
            If the name is not in the fixed relation list.
        """
        forward_name(relation)
        return sorted({link.other for link in self.links if link.relation == relation})


@dataclass(frozen=True, slots=True)
class RawText:
    """A record's raw source text, exactly as served, and its ``content_hash``."""

    text: str
    content_hash: str


@dataclass(frozen=True, slots=True)
class SupplementaryFile:
    """One supplementary file a GEO record lists; listed, never downloaded.

    Parameters
    ----------
    owner : str
        The accession whose SOFT names the file, or the Series whose archive holds a file
        no SOFT line names.
    name : str
        The file name.
    url : str or None
        The file's URL as SOFT gives it; ``None`` for a file reachable only inside an archive.
    size : int or None
        Bytes, from the Series' ``filelist.txt``; ``None`` when that file does not list it.
    date : str or None
        The file's time in ``filelist.txt``, as ``YYYY-MM-DDTHH:MM:SS``.
    type : str or None
        The file type ``filelist.txt`` gives, such as ``TAR`` or ``FA``.
    archive : str or None
        The archive that holds the file, such as ``GSE15556_RAW.tar``.
    """

    owner: str
    name: str
    url: str | None
    size: int | None
    date: str | None
    type: str | None
    archive: str | None


@dataclass(frozen=True, slots=True)
class Partial[T]:
    """A walk's result with ``partial=True``: the held part and what it could not reach.

    Parameters
    ----------
    held : T
        What the default call would return, built from held records only.
    missing : tuple of str
        Every accession the walk reached but the store does not hold, sorted. Empty when
        nothing was missing.
    """

    held: T
    missing: tuple[str, ...]
