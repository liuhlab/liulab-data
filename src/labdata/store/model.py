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
}

#: Reverse relation name to its forward name.
INVERSE: dict[str, str] = {reverse: forward for forward, reverse in RELATIONS.items()}

#: A paper's link target: ``PMID:<n>``. A paper is not a record.
PAPER = "paper"

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
        One of the nine record types, or ``"paper"`` for ``PMID:<n>``.

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
