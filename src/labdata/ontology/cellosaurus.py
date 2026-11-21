"""Read Cellosaurus's OBO file and its list of deleted ids into snapshot data.

The OBO file is read line by line, not with fastobo, which refuses its header: the ``date:``
line puts the month first, and comment lines follow the header's clauses. Each term gives its
id, name and synonyms; the file marks no term obsolete, so retired ids come from the deleted list.
"""

from __future__ import annotations

import re
from pathlib import Path

from labdata.exceptions import OntologyError
from labdata.ontology.tables import (
    Reason,
    RetiredEntry,
    Scope,
    SnapshotData,
    SourceTerm,
    Synonym,
)
from labdata.ontology.vocabularies import Vocabulary, release_of

_SYNONYM = re.compile(r'"((?:[^"\\]|\\.)*)"\s+(EXACT|NARROW|BROAD|RELATED)(?:\s+([^\s\[]+))?')
_ESCAPE = re.compile(r"\\(.)")
_DELETED = re.compile(r"(CVCL_\w+)\s")


def read_cellosaurus(obo: Path, deleted: Path, vocabulary: Vocabulary) -> SnapshotData:
    """Read the cell lines from ``obo``, and each id in ``deleted`` as a deleted retired id.

    Raises
    ------
    OntologyError
        If the header states no ``data-version``, or a synonym does not read.
    """
    version: str | None = None
    terms: list[SourceTerm] = []
    fields: dict[str, list[str]] | None = None
    in_header = True
    with obo.open(encoding="utf-8") as lines:
        for line in lines:
            tag, _, value = line.rstrip("\n").partition(":")
            if line.startswith("["):
                in_header = False
                _keep(terms, fields, vocabulary, obo)
                fields = {} if line.startswith("[Term]") else None
            elif in_header and tag == "data-version":
                version = value.strip()
            elif fields is not None and tag in ("id", "name", "synonym"):
                fields.setdefault(tag, []).append(value.strip())
    _keep(terms, fields, vocabulary, obo)
    if version is None:
        raise OntologyError(f"{obo.name}: the OBO header states no data-version")
    retired = [RetiredEntry(id, None, Reason.DELETED) for id in _deleted(deleted)]
    return SnapshotData(
        vocabulary.name, release_of(version), terms, retired, prefixes=vocabulary.prefixes
    )


def _keep(
    terms: list[SourceTerm], fields: dict[str, list[str]] | None, vocabulary: Vocabulary, obo: Path
) -> None:
    if not fields or not fields.get("id"):
        return
    id = fields["id"][0]
    if not id.startswith(vocabulary.prefixes):
        return
    names = fields.get("name") or [""]
    synonyms = []
    for value in fields.get("synonym", ()):
        found = _SYNONYM.match(value)
        if found is None:
            raise OntologyError(f"{obo.name}: {id} has a synonym that does not read: {value!r}")
        text, scope, kind = found.groups()
        synonyms.append(Synonym(_ESCAPE.sub(r"\1", text), Scope(scope), kind))
    terms.append(SourceTerm(id, names[0] or None, synonyms=tuple(synonyms)))


def _deleted(path: Path) -> list[str]:
    # Lines before the ids are a banner; each id line is "CVCL_xxxx  <why>".
    with path.open(encoding="utf-8") as lines:
        return [m.group(1) for line in lines if (m := _DELETED.match(line))]
