"""Read an OBO file's release from its header, and its own terms into snapshot data.

The file is read frame by frame with fastobo, so a large vocabulary is never held whole as
parsed frames; only the kept terms are.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

# fastobo is compiled and ships no type stubs, so the checker cannot see its names.
from fastobo import header, term  # pyright: ignore[reportAttributeAccessIssue]
from fastobo import iter as frames_of  # pyright: ignore[reportAttributeAccessIssue]

from labdata.exceptions import OntologyError
from labdata.ontology.tables import SnapshotData, SourceTerm, Synonym
from labdata.ontology.vocabularies import Vocabulary, release_of

_TAG = "data-version:"


def header_version(head: bytes) -> str:
    """Return the ``data-version`` of an OBO header, given the file's first bytes.

    Raises
    ------
    OntologyError
        If the header ends, or the bytes run out, before a ``data-version`` line.
    """
    for line in head.decode("utf-8", errors="replace").splitlines():
        if line.startswith("["):
            break
        if line.startswith(_TAG):
            return line.removeprefix(_TAG).strip()
    raise OntologyError("the OBO header states no data-version")


def read_obo(path: Path, vocabulary: Vocabulary) -> SnapshotData:
    """Read the vocabulary's own terms from an OBO file, with the release its header states.

    Terms whose id has none of the vocabulary's prefixes, such as imported ones, are left out.

    Raises
    ------
    OntologyError
        If the file does not parse, or its header states no ``data-version``.
    """
    try:
        frames = frames_of(str(path))
        version = next(
            (str(c.version) for c in frames.header() if isinstance(c, header.DataVersionClause)),
            None,
        )
        terms = [
            _term(frame)
            for frame in frames
            if isinstance(frame, term.TermFrame) and str(frame.id).startswith(vocabulary.prefixes)
        ]
    except (SyntaxError, ValueError) as err:
        raise OntologyError(f"{path.name} does not parse as OBO: {err}") from err
    if version is None:
        raise OntologyError(f"{path.name}: the OBO header states no data-version")
    return SnapshotData(vocabulary.name, release_of(version), terms, prefixes=vocabulary.prefixes)


def _term(frame: Any) -> SourceTerm:
    name: str | None = None
    obsolete = False
    replaced_by: list[str] = []
    alt_ids: list[str] = []
    synonyms: list[Synonym] = []
    for clause in frame:
        if isinstance(clause, term.NameClause):
            name = str(clause.name)
        elif isinstance(clause, term.SynonymClause):
            syn = clause.synonym
            kind = None if syn.type is None else str(syn.type)
            synonyms.append(Synonym(str(syn.desc), str(syn.scope), kind))
        elif isinstance(clause, term.IsObsoleteClause):
            obsolete = bool(clause.obsolete)
        elif isinstance(clause, term.ReplacedByClause):
            replaced_by.append(str(clause.term))
        elif isinstance(clause, term.AltIdClause):
            alt_ids.append(str(clause.alt_id))
    return SourceTerm(
        str(frame.id), name, obsolete, tuple(replaced_by), tuple(alt_ids), tuple(synonyms)
    )
