"""Raw text in buckets: fetch lanes write parts under ``work/parts/``, pack merges them.

A raw row is ``accession``, ``raw_text``, ``hash`` and ``fetch_date``. A source's rows sit in
``raw/<source>/<bucket>.parquet``, a bucket being a range of :data:`WIDTHS` accession numbers.

A fetch lane splits its work into :class:`Piece` s, one per bucket, and writes each finished
piece as one part file that appears by a rename. A rerun skips every accession a part or a
raw bucket already holds, so a saved piece makes no request. Only :mod:`labdata.store.pack`
writes ``raw/``. Writer side only, so pyarrow is imported here freely.
"""

from __future__ import annotations

import hashlib
import os
import re
import threading
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

#: Raw folder of the SRA ``EXPERIMENT_PACKAGE`` XML, one row per experiment.
SRA_XML = "sra-xml"
#: Raw folder of PubMed ``<accession>[si]`` results, one row per queried accession.
PUBMED_SI = "pubmed-si"
#: Raw folder of decompressed GEO family SOFT, one row per Series.
GEO_SOFT = "geo-soft"
#: Raw folder of ``elink`` results, one row per Series: tab-separated lines of
#: from accession, target database, target UID and target accession.
ENTREZ_LINKS = "entrez-links"
#: Raw folder of each Series' ``suppl/filelist.txt``, one row per Series; empty when absent.
GEO_FILELIST = "geo-filelist"
#: Raw folder of each GEO Series the fetch could not save, one row per Series: its gap
#: reason, then any detail after a newline (see :mod:`labdata.store.gaps`).
GEO_GAPS = "geo-gaps"

#: Accession numbers per bucket, a power of ten per source, set from the pilot store's
#: Parquet bytes so a full-scale bucket file lands at about 50 MB to 1 GB.
WIDTHS: dict[str, int] = {
    GEO_SOFT: 10_000,
    GEO_FILELIST: 100_000,
    SRA_XML: 1_000_000,
    PUBMED_SI: 1_000_000,
    ENTREZ_LINKS: 1_000_000,
    GEO_GAPS: 1_000_000,
}
#: Raw text held in memory before it is written as one sorted row group.
CHUNK_BYTES = 32 << 20
#: Most rows sorted as one chunk.
CHUNK_ROWS = 10_000
#: Row groups one chunk is written as.
GROUPS_PER_CHUNK = 8
#: The parquet key listing the accessions a part's piece asked for, one per line.
ASKED_KEY = b"labdata.asked"

SCHEMA = pa.schema(
    [
        ("accession", pa.string()),
        ("raw_text", pa.string()),
        ("hash", pa.string()),
        ("fetch_date", pa.string()),
    ]
)
_ACCESSION = re.compile(r"^([A-Z]+)(\d+)$")


def sha256(text: str) -> str:
    """Return the hex sha256 of a text's UTF-8 bytes: every ``hash`` and ``content_hash``."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def bucket_of(source: str, accession: str) -> str:
    """Return the bucket an accession's rows go in within one source.

    The bucket keeps the accession's prefix and its number with the last digits of the
    source's width written ``n``: at width 10,000, ``GSE246310`` -> ``GSE24nnnn``.

    Raises
    ------
    ValueError
        If the accession is not letters then digits.
    KeyError
        If the source has no width in :data:`WIDTHS`.
    """
    digits = len(str(WIDTHS[source])) - 1
    if match := _ACCESSION.match(accession):
        return f"{match[1]}{match[2][:-digits]}{'n' * digits}"
    raise ValueError(f"no raw bucket for {accession!r}")


@dataclass(frozen=True, slots=True)
class RawRow:
    """One raw item: the accession it was fetched for, its text and the fetch date."""

    accession: str
    raw_text: str
    fetch_date: str


@dataclass(frozen=True, slots=True)
class Piece:
    """One unit of fetch work: the unsaved accessions of one source's bucket."""

    source: str
    bucket: str
    todo: tuple[str, ...]


def parts_dir(root: Path, source: str) -> Path:
    """Return the folder of a source's parts, ``work/parts/<source>/``."""
    return root / "work" / "parts" / source


def part_files(root: Path, source: str) -> list[Path]:
    """Return a source's finished part files, oldest first."""
    folder = parts_dir(root, source)
    if not folder.is_dir():
        return []
    files = [p for p in folder.glob("*.parquet") if not p.name.startswith(".")]
    return sorted(files, key=lambda p: (p.stat().st_mtime_ns, p.name))


def part_bucket(path: Path) -> str:
    """Return the bucket a part file belongs to (its name up to the first dot)."""
    return path.name.split(".", 1)[0]


def pieces(root: Path, source: str, accessions: Iterable[str]) -> list[Piece]:
    """Split accessions into pieces, one per bucket, leaving out every saved accession.

    An accession is saved when a raw bucket row or a part holds it, or a part's piece asked
    for it and got no row. A bucket with nothing left to fetch yields no piece.
    """
    by_bucket: dict[str, set[str]] = defaultdict(set)
    for accession in accessions:
        by_bucket[bucket_of(source, accession)].add(accession)
    done = _done(root, source, set(by_bucket))
    return [
        Piece(source, bucket, tuple(sorted(wanted - done[bucket])))
        for bucket, wanted in sorted(by_bucket.items())
        if wanted - done[bucket]
    ]


def _done(root: Path, source: str, buckets: set[str]) -> dict[str, set[str]]:
    done: dict[str, set[str]] = defaultdict(set)
    for bucket in buckets:
        path = root / "raw" / source / f"{bucket}.parquet"
        if path.is_file():
            done[bucket] |= set(pq.read_table(path, columns=["accession"]).column(0).to_pylist())
    for path in part_files(root, source):
        bucket = part_bucket(path)
        if bucket in buckets:
            done[bucket] |= set(pq.read_table(path, columns=["accession"]).column(0).to_pylist())
            done[bucket] |= asked(path)
    return done


def asked(path: Path) -> set[str]:
    """Return the accessions a part's piece asked for."""
    metadata = pq.read_schema(path).metadata or {}
    return set(filter(None, metadata.get(ASKED_KEY, b"").decode().split("\n")))


def fetch_pieces(
    root: Path,
    source: str,
    accessions: Iterable[str],
    get: Callable[[list[str]], Iterable[tuple[str, str]]],
    date: str,
    *,
    remember_misses: bool = True,
) -> list[Path]:
    """Fetch every unsaved piece of a source and write each as a part.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    source : str
        The raw folder name, e.g. ``"sra-xml"``.
    accessions : iterable of str
        Every accession the lane wants saved.
    get : callable
        Takes one piece's accessions and yields ``(accession, raw_text)`` pairs; it may
        stream, as rows are spilled to the part in chunks.
    date : str
        The fetch date recorded on every row.
    remember_misses : bool
        Whether an accession ``get`` gave no row for counts as saved until the next pack.

    Returns
    -------
    list of pathlib.Path
        The parts written; a saved piece writes none and makes no call to ``get``.
    """
    return [
        write_part(
            root,
            piece,
            (RawRow(a, text, date) for a, text in get(list(piece.todo))),
            record_asked=remember_misses,
        )
        for piece in pieces(root, source, accessions)
    ]


def write_part(
    root: Path,
    piece: Piece,
    rows: Iterable[RawRow],
    *,
    record_asked: bool = True,
    chunk_bytes: int = CHUNK_BYTES,
) -> Path:
    """Write one piece's rows as a part file, chunk by chunk, renamed into place when whole.

    Each chunk is sorted by accession and written as one row group, so memory holds one
    chunk. With ``record_asked``, the part records ``piece.todo`` as asked, so an accession
    with no row is not fetched again before the next pack.

    Raises
    ------
    ValueError
        If a row belongs to another bucket.
    """
    folder = parts_dir(root, piece.source)
    folder.mkdir(parents=True, exist_ok=True)
    tag = hashlib.sha256("\n".join(piece.todo).encode()).hexdigest()[:12]
    path = folder / f"{piece.bucket}.{tag}.parquet"

    def checked() -> Iterator[RawRow]:
        for row in rows:
            if bucket_of(piece.source, row.accession) != piece.bucket:
                raise ValueError(f"{row.accession} is not in bucket {piece.bucket}")
            yield row

    listed = "\n".join(piece.todo) if record_asked else ""
    schema = SCHEMA.with_metadata({ASKED_KEY: listed.encode()})
    write_sorted_runs(path, checked(), schema, chunk_bytes=chunk_bytes)
    return path


def write_sorted_runs(
    path: Path, rows: Iterable[RawRow], schema: pa.Schema, *, chunk_bytes: int = CHUNK_BYTES
) -> None:
    """Write rows to ``path`` one chunk at a time, each chunk sorted by accession.

    A chunk is written as :data:`GROUPS_PER_CHUNK` row groups, so a reader merging many
    sorted runs holds one small row group of each. The file appears by a rename once whole.
    Rows already in order make a sorted file.
    """
    tmp = writer_temp(path)
    try:
        with pq.ParquetWriter(tmp, schema) as writer:
            chunk: list[RawRow] = []
            size = 0
            for row in rows:
                chunk.append(row)
                size += len(row.raw_text)
                if size >= chunk_bytes or len(chunk) >= CHUNK_ROWS:
                    _write_chunk(writer, chunk, schema, chunk_bytes)
                    chunk, size = [], 0
            if chunk:
                _write_chunk(writer, chunk, schema, chunk_bytes)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def _write_chunk(
    writer: pq.ParquetWriter, chunk: list[RawRow], schema: pa.Schema, chunk_bytes: int
) -> None:
    group: list[RawRow] = []
    size = 0
    for row in sorted(chunk, key=_key):
        group.append(row)
        size += len(row.raw_text)
        if size >= chunk_bytes // GROUPS_PER_CHUNK:
            writer.write_table(_table(group, schema))
            group, size = [], 0
    if group:
        writer.write_table(_table(group, schema))


def writer_temp(path: Path) -> Path:
    """Return a hidden temporary path beside ``path``, unique to this process and thread."""
    return path.with_name(f".{path.name}.{os.getpid()}-{threading.get_ident()}.tmp")


def _key(row: RawRow) -> str:
    return row.accession


def _table(rows: list[RawRow], schema: pa.Schema) -> pa.Table:
    return pa.table(
        {
            "accession": [row.accession for row in rows],
            "raw_text": [row.raw_text for row in rows],
            "hash": [sha256(row.raw_text) for row in rows],
            "fetch_date": [row.fetch_date for row in rows],
        },
        schema=schema,
    )


def saved(root: Path, source: str, accessions: Iterable[str]) -> dict[str, str]:
    """Return the saved raw text of those ``accessions`` that have a row, in raw or a part.

    A part's row wins over a raw bucket's, and a newer part's over an older one's.
    """
    wanted = set(accessions)
    buckets = {bucket_of(source, a) for a in wanted}
    paths = [root / "raw" / source / f"{bucket}.parquet" for bucket in sorted(buckets)]
    paths += [p for p in part_files(root, source) if part_bucket(p) in buckets]
    out: dict[str, str] = {}
    for path in paths:
        out |= {row.accession: row.raw_text for row in _read(path) if row.accession in wanted}
    return out


def read_rows(root: Path, source: str) -> list[RawRow]:
    """Return every raw row of one source, sorted by accession."""
    folder = root / "raw" / source
    rows = [row for path in sorted(folder.glob("*.parquet")) for row in _read(path)]
    return sorted(rows, key=_key)


def _read(path: Path) -> list[RawRow]:
    if not path.is_file():
        return []
    data = pq.read_table(path, columns=["accession", "raw_text", "fetch_date"]).to_pydict()
    return [
        RawRow(accession, text, date)
        for accession, text, date in zip(
            data["accession"], data["raw_text"], data["fetch_date"], strict=True
        )
    ]
