"""Read NCBI Taxonomy's dump, ``taxdump.tar.gz``, into snapshot data, and check its checksum.

The tarball is read member by member as a stream; nothing is extracted to disk.
"""

from __future__ import annotations

import hashlib
import io
import tarfile
from collections.abc import Iterator
from pathlib import Path

from labdata.exceptions import OntologyError
from labdata.ontology.tables import Reason, RetiredEntry, SnapshotData, SourceTerm
from labdata.ontology.vocabularies import TAXON_PREFIX, Vocabulary

_SCIENTIFIC = "scientific name"
_CHUNK = 1 << 20


def check_md5(path: Path, listed: bytes) -> None:
    """Check ``path`` against an ``.md5`` file's text, ``<hex digest>  <file name>``.

    Raises
    ------
    OntologyError
        If the digests differ, or the ``.md5`` text holds none.
    """
    words = listed.decode("ascii", errors="replace").split()
    if not words:
        raise OntologyError(f"{path.name}: its .md5 file is empty")
    digest = hashlib.md5()
    with path.open("rb") as f:
        while chunk := f.read(_CHUNK):
            digest.update(chunk)
    if digest.hexdigest() != words[0].lower():
        raise OntologyError(f"{path.name} fails its checksum: the .md5 file lists {words[0]}")


def read_taxdump(path: Path, vocabulary: Vocabulary, release: str) -> SnapshotData:
    """Read every taxon's scientific name, and the merged and deleted ids, from the dump.

    Ids are stored as ``NCBITaxon:<number>``.

    Raises
    ------
    OntologyError
        If the file is no readable tarball, or lacks ``names.dmp``, ``merged.dmp`` or
        ``delnodes.dmp``.
    """
    try:
        with tarfile.open(path, "r:gz") as tar:
            terms = [
                SourceTerm(_id(row[0]), row[1])
                for row in _rows(tar, "names.dmp")
                if row[3] == _SCIENTIFIC
            ]
            retired = [
                RetiredEntry(_id(old), _id(new), Reason.MERGED)
                for old, new, *_ in _rows(tar, "merged.dmp")
            ]
            retired += [
                RetiredEntry(_id(row[0]), None, Reason.DELETED)
                for row in _rows(tar, "delnodes.dmp")
            ]
    except (tarfile.TarError, OSError, EOFError, IndexError, UnicodeDecodeError) as err:
        raise OntologyError(f"{path.name} does not read as a taxonomy dump: {err}") from err
    return SnapshotData(
        vocabulary.name,
        release,
        terms,
        retired,
        prefixes=vocabulary.prefixes,
        searchable=vocabulary.searchable,
    )


def _rows(tar: tarfile.TarFile, member: str) -> Iterator[list[str]]:
    # A .dmp line is fields split by "\t|\t", ending in "\t|".
    try:
        raw = tar.extractfile(member)
    except KeyError:
        raise OntologyError(f"the taxonomy dump has no {member}") from None
    if raw is None:
        raise OntologyError(f"the taxonomy dump's {member} is not a file")
    with io.TextIOWrapper(raw, encoding="utf-8") as lines:
        for line in lines:
            yield line.rstrip("\n").removesuffix("\t|").split("\t|\t")


def _id(number: str) -> str:
    return f"{TAXON_PREFIX}{number.strip()}"
