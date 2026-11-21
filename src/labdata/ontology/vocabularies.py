"""The vocabularies the store keeps: one typed entry each, and how a release is named.

A plain OBO Foundry vocabulary needs only its name and its own-term id prefixes; the address
and the version rule default to the OBO Foundry file and its ``data-version`` header.
"""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass

from labdata.exceptions import OntologyError

#: The address of an OBO Foundry vocabulary's OBO file.
OBO_FOUNDRY = "https://purl.obolibrary.org/obo/{name}.obo"

_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


class ReaderKind(enum.StrEnum):
    """How a vocabulary's source file is read into terms."""

    OBO = "obo"


class VersionRule(enum.StrEnum):
    """Where a vocabulary states its release."""

    #: The ``data-version`` line of the OBO header.
    DATA_VERSION = "data-version"


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
    """

    name: str
    prefixes: tuple[str, ...]
    url: str = ""
    reader: ReaderKind = ReaderKind.OBO
    version: VersionRule = VersionRule.DATA_VERSION

    def __post_init__(self) -> None:
        """Fill in the OBO Foundry address when none is given."""
        if not self.url:
            object.__setattr__(self, "url", OBO_FOUNDRY.format(name=self.name))


#: Every vocabulary the update brings in.
VOCABULARIES: tuple[Vocabulary, ...] = (Vocabulary("fbdv", ("FBdv:",)),)


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
