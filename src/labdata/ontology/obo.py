"""Read an OBO file's release from its header, and its own terms into snapshot data.

The file is read frame by frame with fastobo, so a large vocabulary is never held whole as
parsed frames; only the kept terms are. fastobo is given only the clauses the reader reads.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

# fastobo is compiled and ships no type stubs, so the checker cannot see its names.
from fastobo import header, term  # pyright: ignore[reportAttributeAccessIssue]
from fastobo import iter as frames_of  # pyright: ignore[reportAttributeAccessIssue]

from labdata.exceptions import OntologyError
from labdata.ontology.tables import Scope, SnapshotData, SourceTerm, Synonym
from labdata.ontology.vocabularies import Vocabulary, release_of

_TAG = "data-version:"
# A type named only by a number, such as OMO:0003004 ("plural form"), reads by its header name;
# a named one, such as UBERON's INCONSISTENT, whose header text is a definition, reads as is.
_NUMBERED = re.compile(r"[A-Za-z]+:\d+")
#: The ``<PREFIX>_<number>`` that ends an IRI-shaped id, such as ``OBA_VT0000047``.
_LOCAL = re.compile(r"([A-Za-z][A-Za-z0-9]*)_([A-Za-z]*\d+)")
#: A synonym clause up to the ``[`` that opens its xref list.
_SYNONYM_HEAD = re.compile(rb'\s*synonym:\s*"(?:[^"\\]|\\.)*"[^[\n]*\[')
#: The starts of the lines fastobo is given: a frame's first line, or a clause the reader reads.
_READ = (
    b"[",
    b"id:",
    b"name:",
    b"synonym:",
    b"is_obsolete:",
    b"replaced_by:",
    b"alt_id:",
    b"is_a:",
    b"data-version:",
    b"synonymtypedef:",
)


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


def read_obo(path: Path, vocabulary: Vocabulary, *, release: str | None = None) -> SnapshotData:
    """Read the vocabulary's own terms from an OBO file, with the release its header states.

    Terms whose id has none of the vocabulary's prefixes, such as imported ones, are left out;
    an own term's ``is_a`` parents are kept whatever their prefix.
    An id spelled with an IRI's ending, ``efo:EFO_0008896`` or ``http://.../EFO_0008896``,
    reads as ``EFO:0008896``.
    A given ``release`` is used instead of the header's, for a file that states none. A synonym
    type named only by a number, such as ``OMO:0003004``, is stored by its name in the header.

    Raises
    ------
    OntologyError
        If the file does not parse, or no release is given and its header states no
        ``data-version``.
    """
    try:
        with path.open("rb") as lines:
            frames = frames_of(_ReadLines(lines))
            head = list(frames.header())
            version = next(
                (str(c.version) for c in head if isinstance(c, header.DataVersionClause)), None
            )
            labels = _type_labels(head)
            terms = [
                _term(frame, labels)
                for frame in frames
                if isinstance(frame, term.TermFrame)
                and _curie(frame.id).startswith(vocabulary.prefixes)
            ]
    except (SyntaxError, ValueError) as err:
        raise OntologyError(f"{path.name} does not parse as OBO: {_detail(err)}") from err
    if release is None and version is None:
        raise OntologyError(f"{path.name}: the OBO header states no data-version")
    stated = release if release is not None else release_of(str(version))
    return SnapshotData(vocabulary.name, stated, terms, prefixes=vocabulary.prefixes)


class _ReadLines:
    """The lines of an OBO file that open a frame or hold a clause the reader reads.

    fastobo then never parses a clause the store does not keep, such as an ``xref`` whose bare
    IRI holds an OBO escape, which its grammar rejects. Two kept clauses are mended as EFO
    writes them: a name that starts with a bare ``!``, which fastobo reads as a comment, gets it
    escaped, and a synonym's xrefs, which may hold a space, are dropped.
    """

    def __init__(self, lines: Iterable[bytes]) -> None:
        self._lines = (_mended(line) for line in lines if line.lstrip(b" \t").startswith(_READ))
        self._held = bytearray()

    def read(self, size: int) -> bytes:
        """Return the next ``size`` bytes, or fewer at the end."""
        while len(self._held) < size and (line := next(self._lines, None)) is not None:
            self._held += line
        given = bytes(self._held[:size])
        del self._held[:size]
        return given


def _mended(line: bytes) -> bytes:
    """Return a kept line as fastobo can parse it."""
    clause = line.lstrip(b" \t")
    if clause.startswith(b"name:"):
        tag, _, value = line.partition(b":")
        if value.lstrip(b" \t").startswith(b"!"):
            return tag + b": \\" + value.lstrip(b" \t")
    if clause.startswith(b"synonym:") and (head := _SYNONYM_HEAD.match(line)):
        return head.group(0) + b"]\n"
    return line


def _detail(err: Exception) -> str:
    """Say why fastobo failed by the clause it stopped at, as its line number is not the file's."""
    if isinstance(err, SyntaxError) and err.text:
        return f"{err.msg} at {err.text.strip()!r}"
    return str(err)


def _curie(ident: Any) -> str:
    """Return an id, or the ``PREFIX:number`` its IRI-shaped spelling stands for."""
    text = str(ident)
    if "_" not in text:
        return text
    if "://" in text:
        local = text.rpartition("/")[2]
    else:
        space, _, local = text.partition(":")
        if not local.lower().startswith(f"{space.lower()}_"):
            return text
    found = _LOCAL.fullmatch(local)
    return f"{found.group(1)}:{found.group(2)}" if found else text


def _type_labels(head: list[Any]) -> dict[str, str]:
    """Map each numbered synonym type, such as ``OMO:0003004``, to its name in the header."""
    labels = {}
    for clause in head:
        if isinstance(clause, header.SynonymTypedefClause):
            id, name = str(clause.typedef), str(clause.description)
            if _NUMBERED.fullmatch(id) and name:
                labels[id] = name
    return labels


def _term(frame: Any, labels: dict[str, str]) -> SourceTerm:
    name: str | None = None
    obsolete = False
    replaced_by: list[str] = []
    alt_ids: list[str] = []
    synonyms: list[Synonym] = []
    parents: list[str] = []
    for clause in frame:
        if isinstance(clause, term.NameClause):
            name = str(clause.name)
        elif isinstance(clause, term.SynonymClause):
            syn = clause.synonym
            kind = None if syn.type is None else labels.get(str(syn.type), str(syn.type))
            synonyms.append(Synonym(str(syn.desc), Scope(str(syn.scope)), kind))
        elif isinstance(clause, term.IsObsoleteClause):
            obsolete = bool(clause.obsolete)
        elif isinstance(clause, term.ReplacedByClause):
            replaced_by.append(_curie(clause.term))
        elif isinstance(clause, term.AltIdClause):
            alt_ids.append(_curie(clause.alt_id))
        elif isinstance(clause, term.IsAClause):
            parents.append(_curie(clause.term))
    return SourceTerm(
        _curie(frame.id),
        name,
        obsolete,
        tuple(replaced_by),
        tuple(alt_ids),
        tuple(synonyms),
        tuple(parents),
    )
