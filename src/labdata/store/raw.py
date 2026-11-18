"""What ``raw/`` and its parts hold: one merge answers every read, and pack writes it.

A raw row is ``accession``, ``raw_text``, ``hash`` and ``fetch_date``. A source's rows sit in
``raw/<source>/<bucket>.parquet``, a bucket being a range of :data:`WIDTHS` accession numbers.
A fetch saves rows as a part under ``work/parts/``, and a removal as a removal part; pack
merges them into the bucket files.

Every read and pack share :func:`merge`: the bucket file, then the parts oldest first, the
newest row of an accession winning and a removal dropping the rows saved before it. So an
accession is held, or left to fetch, alike before and after pack. Writer side only, so pyarrow
is imported here freely.
"""

from __future__ import annotations

import bisect
import hashlib
import heapq
import itertools
import os
import re
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from operator import itemgetter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata.store import paths

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
#: Raw folder of each SRA experiment the E-utilities lane asked for and could not fetch, one
#: row per experiment: the last error. The build notes it while ``sra-xml`` holds none.
SRA_FETCH_FAILED = "sra-fetch-failed"

#: Accession numbers per bucket, a power of ten per source.
WIDTHS: dict[str, int] = {
    GEO_SOFT: 10_000,
    GEO_FILELIST: 100_000,
    SRA_XML: 1_000_000,
    PUBMED_SI: 1_000_000,
    ENTREZ_LINKS: 1_000_000,
    GEO_GAPS: 1_000_000,
    SRA_FETCH_FAILED: 1_000_000,
}
#: Raw text sorted in memory as one chunk before it is written.
SORT_CHUNK_BYTES = 32 << 20
#: Most rows sorted as one chunk.
SORT_CHUNK_ROWS = 10_000
#: Row groups one chunk is written as.
GROUPS_PER_CHUNK = 8
#: Rows read from a file at a time when they hold raw text.
READ_ROWS = 16
#: Rows read from a file at a time when they hold no raw text.
_NARROW_ROWS = 1 << 16
#: The parquet key listing the accessions a part's piece asked for, one per line.
ASKED_KEY = b"labdata.asked"
#: The parquet key listing the accessions a removal part deletes, one per line.
REMOVED_KEY = b"labdata.removed"
#: The ``fetch_date`` that marks a row as a removal in a file pack merges through.
REMOVED = "removed"
#: The columns a merged row's values hold, after its accession.
ROW = ("raw_text", "fetch_date")

SCHEMA = pa.schema(
    [
        ("accession", pa.string()),
        ("raw_text", pa.string()),
        ("hash", pa.string()),
        ("fetch_date", pa.string()),
    ]
)
_ACCESSION = re.compile(r"^([A-Z]+)(\d+)$")
#: Digits of the write stamp in a part's name, ``<bucket>.<stamp>.<tag>.parquet``.
_STAMP_DIGITS = 20
_last_stamp = 0
_stamp_lock = threading.Lock()


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
    """One unit of fetch work: one source's bucket's unsaved and re-fetch accessions."""

    source: str
    bucket: str
    todo: tuple[str, ...]


def parts_dir(root: Path, source: str) -> Path:
    """Return the folder of a source's parts, ``work/parts/<source>/``."""
    return paths.PARTS.at(root, source)


def part_files(root: Path, source: str) -> list[Path]:
    """Return a source's finished part files, oldest first.

    Oldest is by the stamp in the name, and a part without one by its mtime. Across
    processes, the order follows each writer's clock.
    """
    folder = parts_dir(root, source)
    if not folder.is_dir():
        return []
    files = [p for p in folder.glob("*.parquet") if not p.name.startswith(".")]
    return sorted(files, key=_order_key)


def parts_by_bucket(root: Path, source: str) -> dict[str, list[Path]]:
    """Return a source's part files by bucket, each bucket's oldest first."""
    out: dict[str, list[Path]] = defaultdict(list)
    for path in part_files(root, source):
        out[_name_fields(path)[0]].append(path)
    return dict(out)


def _order_key(path: Path) -> tuple[int, str]:
    stamp = _name_fields(path)[1]
    return (path.stat().st_mtime_ns if stamp is None else stamp, path.name)


def part_name(bucket: str, tag: str, stamp: int | None = None) -> str:
    """Return a part's file name, ``<bucket>.<stamp>.<tag>.parquet``.

    With no stamp it is ``<bucket>.<tag>.parquet``, and the part sorts by its mtime.
    """
    if stamp is None:
        return f"{bucket}.{tag}.parquet"
    return f"{bucket}.{stamp:0{_STAMP_DIGITS}d}.{tag}.parquet"


def _name_fields(path: Path) -> tuple[str, int | None, str]:
    """Return a part name's bucket, stamp (None when it has none) and tag."""
    bucket, _, rest = path.name.removesuffix(".parquet").partition(".")
    stamp, dot, tag = rest.partition(".")
    if dot and len(stamp) == _STAMP_DIGITS and stamp.isdigit():
        return bucket, int(stamp), tag
    return bucket, None, rest


def _parts_of(folder: Path, bucket: str, tag: str) -> list[Path]:
    """Return the parts of one bucket and tag, stamped or not."""
    return [p for p in folder.glob(f"{bucket}.*{tag}.parquet") if _name_fields(p)[2] == tag]


class _Mark:
    """A merge entry that holds no row."""

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return self.name


#: The entry of an accession a removal part lists: the rows saved before it are dropped.
GONE = _Mark("gone")
#: The entry of an accession a part's piece asked for.
_ASKED = _Mark("asked")

type Value = tuple[str, ...] | _Mark
type Run = Iterable[tuple[str, Value]]


@dataclass(frozen=True, slots=True)
class _Wanted:
    """The accessions a read is limited to, as a set and in order."""

    members: frozenset[str]
    ordered: list[str]

    @classmethod
    def of(cls, accessions: Iterable[str]) -> _Wanted:
        members = frozenset(accessions)
        return cls(members, sorted(members))

    def may_be_in(self, group: pq.RowGroupMetaData) -> bool:
        """Return whether a row group's accession range takes in any wanted accession."""
        stats = group.column(0).statistics
        if not (stats and stats.has_min_max):
            return True
        at = bisect.bisect_left(self.ordered, stats.min)
        return at < len(self.ordered) and self.ordered[at] <= stats.max


def merge(runs: Sequence[Run]) -> Iterator[tuple[str, list[tuple[int, Value]]]]:
    """Merge runs sorted by accession, oldest first, into each accession's entries.

    An entry is ``(run index, value)``, in run order; within a run, in row order.
    """
    streams = [_ranked(rank, run) for rank, run in enumerate(runs)]
    merged = streams[0] if len(streams) == 1 else heapq.merge(*streams, key=itemgetter(0, 1))
    for accession, group in itertools.groupby(merged, key=itemgetter(0)):
        yield accession, [(rank, value) for _, rank, value in group]


def _ranked(rank: int, run: Run) -> Iterator[tuple[str, int, Value]]:
    return ((accession, rank, value) for accession, value in run)


def newest(entries: list[tuple[int, Value]]) -> tuple[int, Value]:
    """Return the entry that decides an accession: its newest row or removal.

    The value is :data:`GONE` when a removal is newer than every row, or there is no row.
    """
    for rank, value in reversed(entries):
        if value is not _ASKED:
            return rank, value
    return -1, GONE


def layers(
    root: Path,
    source: str,
    bucket: str,
    parts: list[Path],
    columns: tuple[str, ...] = ROW,
    *,
    packed: bool = True,
    asked: bool = False,
    wanted: _Wanted | None = None,
) -> tuple[list[Run], int]:
    """Return a bucket's runs for :func:`merge`, oldest first, and how many its bucket file gave.

    The bucket file is one run, unless ``packed`` is false; each of ``parts`` follows, with
    ``asked`` the accessions its piece asked for as one more run.
    """
    out: list[Run] = []
    path = paths.RAW.at(root, source, f"{bucket}.parquet")
    if packed and path.is_file():
        out.append(file_run(path, columns, wanted))
    first = len(out)
    for part in parts:
        out += runs(part, columns, wanted)
        if asked:
            out.append([(a, _ASKED) for a in sorted(_listed(part, ASKED_KEY)) if _in(a, wanted)])
    return out, first


def runs(path: Path, columns: tuple[str, ...] = ROW, wanted: _Wanted | None = None) -> list[Run]:
    """Return a part's sorted runs, oldest first: its row groups in order, by their stats.

    A removal part is one run of :data:`GONE`.
    """
    if gone := _listed(path, REMOVED_KEY):
        return [[(a, GONE) for a in sorted(gone) if _in(a, wanted)]]
    metadata = pq.read_metadata(path)
    out: list[Run] = []
    run: list[int] = []
    last: str | None = None
    for group in range(metadata.num_row_groups):
        stats = metadata.row_group(group).column(0).statistics
        low, high = (stats.min, stats.max) if stats and stats.has_min_max else (None, None)
        if run and (low is None or last is None or low < last):
            out.append(file_run(path, columns, wanted, run))
            run = []
        run.append(group)
        last = high
    if run:
        out.append(file_run(path, columns, wanted, run))
    return out


def file_run(
    path: Path,
    columns: tuple[str, ...] = ROW,
    wanted: _Wanted | None = None,
    groups: list[int] | None = None,
) -> Iterator[tuple[str, Value]]:
    """Yield a file's ``(accession, values)``, or some row groups', in file order.

    A row dated :data:`REMOVED` yields :data:`GONE`. Raw text is read a few rows at a time,
    and only from row groups whose accession range takes in a ``wanted`` one.
    """
    text = "raw_text" in columns
    dated = columns.index("fetch_date") if "fetch_date" in columns else None
    with pq.ParquetFile(path, pre_buffer=False) as fh:
        groups = list(range(fh.num_row_groups)) if groups is None else groups
        if wanted is not None and text:
            groups = [g for g in groups if wanted.may_be_in(fh.metadata.row_group(g))]
        if not groups:
            return
        for batch in fh.iter_batches(
            batch_size=READ_ROWS if text else _NARROW_ROWS,
            row_groups=groups,
            columns=["accession", *columns],
        ):
            data = batch.to_pydict()
            keys = data.pop("accession")
            if not columns:
                yield from ((a, ()) for a in keys if _in(a, wanted))
                continue
            for accession, *values in zip(keys, *data.values(), strict=True):
                if not _in(accession, wanted):
                    continue
                gone = dated is not None and values[dated] == REMOVED
                yield accession, GONE if gone else tuple(values)


def _in(accession: str, wanted: _Wanted | None) -> bool:
    return wanted is None or accession in wanted.members


def _saved(
    root: Path,
    source: str,
    accessions: Iterable[str] | None,
    columns: tuple[str, ...],
    since_pack: bool,
) -> Iterator[tuple[str, tuple[str, ...]]]:
    """Yield each held accession's newest row values, bucket by bucket in accession order."""
    for accession, entries in _merged(root, source, accessions, columns, since_pack):
        _, value = newest(entries)
        if isinstance(value, tuple):
            yield accession, value


def _merged(
    root: Path,
    source: str,
    accessions: Iterable[str] | None,
    columns: tuple[str, ...],
    since_pack: bool,
) -> Iterator[tuple[str, list[tuple[int, Value]]]]:
    """Yield each accession's merge entries, bucket by bucket in accession order."""
    wanted = None if accessions is None else _Wanted.of(accessions)
    parts = parts_by_bucket(root, source)
    if wanted is not None:
        buckets = {bucket_of(source, a) for a in wanted.members}
    elif since_pack:
        buckets = set(parts)
    else:
        buckets = {p.stem for p in paths.RAW.at(root, source).glob("*.parquet")} | set(parts)
    for bucket in sorted(buckets):
        mine = parts.get(bucket, [])
        found, _ = layers(root, source, bucket, mine, columns, packed=not since_pack, wanted=wanted)
        yield from merge(found)


def held(
    root: Path, source: str, accessions: Iterable[str] | None = None, *, since_pack: bool = False
) -> set[str]:
    """Return which of ``accessions`` a source holds; with None, every accession it holds.

    With ``since_pack``, only rows saved since the last pack count.
    """
    return {accession for accession, _ in _saved(root, source, accessions, (), since_pack)}


def saved(
    root: Path,
    source: str,
    accessions: Iterable[str] | None = None,
    *,
    column: str = "raw_text",
    since_pack: bool = False,
) -> dict[str, str]:
    """Return one column of the newest row of each held accession of ``accessions``.

    With ``since_pack``, only rows saved since the last pack count.
    """
    return {a: values[0] for a, values in _saved(root, source, accessions, (column,), since_pack)}


def scan(
    root: Path,
    source: str,
    accessions: Iterable[str],
    pattern: re.Pattern[str],
    *,
    since_pack: bool = False,
    every_version: bool = False,
) -> set[str]:
    """Return what ``pattern`` finds in the newest saved text of ``accessions``.

    With ``since_pack``, only rows saved since the last pack count. With ``every_version``,
    so do the older rows pack will replace and no removal dropped: what a text was and is.
    """
    found: set[str] = set()
    for _, entries in _merged(root, source, accessions, ("raw_text",), since_pack):
        cut = max((i for i, (_, v) in enumerate(entries) if v is GONE), default=-1)
        versions = [v for _, v in entries[cut + 1 :]] if every_version else [newest(entries)[1]]
        for value in versions:
            if isinstance(value, tuple):
                found.update(pattern.findall(value[0]))
    return found


def rows(
    root: Path, source: str, accessions: Iterable[str] | None = None, *, since_pack: bool = False
) -> Iterator[RawRow]:
    """Yield the newest row of each held accession of ``accessions``, in accession order.

    With None, every held accession's. Memory holds a few rows of each part, never a bucket.
    """
    for accession, (text, date) in _saved(root, source, accessions, ROW, since_pack):
        yield RawRow(accession, text, date)


def removing(root: Path, source: str) -> set[str]:
    """Return the accessions a pending removal drops: no row is saved after it."""
    out: set[str] = set()
    for bucket, parts in parts_by_bucket(root, source).items():
        if not any(_listed(part, REMOVED_KEY) for part in parts):
            continue
        found, _ = layers(root, source, bucket, parts, (), packed=False)
        out |= {a for a, entries in merge(found) if newest(entries)[1] is GONE}
    return out


def merged_files(root: Path, source: str, scratch: Path) -> list[Path]:
    """Return Parquet files that hold exactly what a source holds, one per bucket.

    A bucket with no parts is its bucket file; one with parts is merged into ``scratch``.
    """
    parts = parts_by_bucket(root, source)
    out = []
    for path in sorted(paths.RAW.at(root, source).glob("*.parquet")):
        if path.stem not in parts:
            out.append(path)
    for bucket in sorted(parts):
        scratch.mkdir(parents=True, exist_ok=True)
        out.append(scratch / f"{bucket}.parquet")
        found, _ = layers(root, source, bucket, parts[bucket])
        newer = (
            RawRow(accession, *value)
            for accession, entries in merge(found)
            if isinstance(value := newest(entries)[1], tuple)
        )
        write_sorted_runs(out[-1], newer, SCHEMA)
    return out


def pieces(
    root: Path, source: str, accessions: Iterable[str], refetch: Iterable[str] = ()
) -> list[Piece]:
    """Split accessions into pieces, one per bucket, of those left to fetch.

    An accession is left when the source does not hold it, unless a piece since the last
    removal of it asked for it and got no row. A ``refetch`` accession, wanted too, is left
    unless a row saved since the last pack holds it. A bucket with nothing left yields no piece.
    """
    again = set(refetch)
    by_bucket: dict[str, set[str]] = defaultdict(set)
    for accession in itertools.chain(accessions, again):
        by_bucket[bucket_of(source, accession)].add(accession)
    parts = parts_by_bucket(root, source)
    out = []
    for bucket, members in sorted(by_bucket.items()):
        wanted = _Wanted.of(members)
        found, packed = layers(
            root, source, bucket, parts.get(bucket, []), (), asked=True, wanted=wanted
        )
        done = {a for a, entries in merge(found) if _done(entries, packed, a in again)}
        if left := members - done:
            out.append(Piece(source, bucket, tuple(sorted(left))))
    return out


def _done(entries: list[tuple[int, Value]], packed: int, again: bool) -> bool:
    """Return whether an accession needs no fetch: held, or asked since its last removal."""
    rank, value = newest(entries)
    cut = max((r for r, v in entries if v is GONE), default=-1)
    if any(v is _ASKED and r > cut for r, v in entries):
        return True
    return isinstance(value, tuple) and (rank >= packed or not again)


def _listed(path: Path, key: bytes) -> set[str]:
    metadata = pq.read_schema(path).metadata or {}
    return set(filter(None, metadata.get(key, b"").decode().split("\n")))


def write_removals(root: Path, source: str, accessions: Iterable[str]) -> list[Path]:
    """Write removal parts deleting these accessions' rows from one source, one per bucket.

    A removal drops the rows saved before it, and a row saved after it wins. A removal part
    already written is left as it is, keeping its place.

    Returns
    -------
    list of pathlib.Path
        The removal parts, written or already there.
    """
    by_bucket: dict[str, set[str]] = defaultdict(set)
    for accession in accessions:
        by_bucket[bucket_of(source, accession)].add(accession)
    folder = parts_dir(root, source)
    written = []
    for bucket, members in sorted(by_bucket.items()):
        listed = "\n".join(sorted(members))
        tag = f"removed-{hashlib.sha256(listed.encode()).hexdigest()[:12]}"
        folder.mkdir(parents=True, exist_ok=True)
        if kept := _parts_of(folder, bucket, tag):
            written += kept
            continue
        schema = SCHEMA.with_metadata({REMOVED_KEY: listed.encode()})
        written.append(_write_part_file(folder, bucket, tag, [], schema))
    return written


def fetch_pieces(
    root: Path,
    source: str,
    accessions: Iterable[str],
    get: Callable[[list[str]], Iterable[tuple[str, str]]],
    date: str,
    *,
    remember_misses: bool = True,
) -> list[Path]:
    """Fetch every piece left of a source and write each as a part.

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


def save(
    root: Path, source: str, rows: Iterable[RawRow], *, record_asked: bool = True
) -> list[Path]:
    """Save rows as parts, one per bucket: a row wins over every older row of its accession.

    The rows are held in memory; a fetch that streams its rows writes them with
    :func:`write_part`.

    Returns
    -------
    list of pathlib.Path
        The parts written.
    """
    by_bucket: dict[str, list[RawRow]] = defaultdict(list)
    for row in rows:
        by_bucket[bucket_of(source, row.accession)].append(row)
    return [
        write_part(
            root,
            Piece(source, bucket, tuple(sorted({row.accession for row in chunk}))),
            chunk,
            record_asked=record_asked,
        )
        for bucket, chunk in sorted(by_bucket.items())
    ]


def write_part(
    root: Path,
    piece: Piece,
    rows: Iterable[RawRow],
    *,
    record_asked: bool = True,
    chunk_bytes: int = SORT_CHUNK_BYTES,
) -> Path:
    """Write one piece's rows as a part file, chunk by chunk, renamed into place when whole.

    A part the same piece wrote before is then removed. Each chunk is sorted by accession and
    written as one row group, so memory holds one chunk. With ``record_asked``, the part
    records ``piece.todo`` as asked, so an accession with no row is not fetched again before
    the next pack.

    Raises
    ------
    ValueError
        If a row belongs to another bucket.
    """
    folder = parts_dir(root, piece.source)
    folder.mkdir(parents=True, exist_ok=True)
    tag = hashlib.sha256("\n".join(piece.todo).encode()).hexdigest()[:12]

    def checked() -> Iterator[RawRow]:
        for row in rows:
            if bucket_of(piece.source, row.accession) != piece.bucket:
                raise ValueError(f"{row.accession} is not in bucket {piece.bucket}")
            yield row

    listed = "\n".join(piece.todo) if record_asked else ""
    schema = SCHEMA.with_metadata({ASKED_KEY: listed.encode()})
    return _write_part_file(folder, piece.bucket, tag, checked(), schema, chunk_bytes)


def _write_part_file(
    folder: Path,
    bucket: str,
    tag: str,
    rows: Iterable[RawRow],
    schema: pa.Schema,
    chunk_bytes: int = SORT_CHUNK_BYTES,
) -> Path:
    """Write a part as :func:`write_sorted_runs` does, then remove its earlier copies.

    Its stamp is taken with the rename, under one lock, so within a process stamp order is
    rename order. An earlier copy is an older part of the same bucket and tag.
    """
    global _last_stamp
    tmp = writer_temp(folder / part_name(bucket, tag))
    try:
        _write_runs(tmp, rows, schema, chunk_bytes)
        with _stamp_lock:
            _last_stamp = max(time.time_ns(), _last_stamp + 1)
            path = folder / part_name(bucket, tag, _last_stamp)
            tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    for other in _parts_of(folder, bucket, tag):
        if _order_key(other) < _order_key(path):
            other.unlink(missing_ok=True)
    return path


def write_sorted_runs(
    path: Path, rows: Iterable[RawRow], schema: pa.Schema, *, chunk_bytes: int = SORT_CHUNK_BYTES
) -> None:
    """Write rows to ``path`` one chunk at a time, each chunk sorted by accession.

    A chunk is written as :data:`GROUPS_PER_CHUNK` row groups, so a reader merging many
    sorted runs holds one small row group of each. The file appears by a rename once whole.
    Rows already in order make a sorted file.
    """
    tmp = writer_temp(path)
    try:
        _write_runs(tmp, rows, schema, chunk_bytes)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def _write_runs(path: Path, rows: Iterable[RawRow], schema: pa.Schema, chunk_bytes: int) -> None:
    with pq.ParquetWriter(path, schema) as writer:
        chunk: list[RawRow] = []
        size = 0
        for row in rows:
            chunk.append(row)
            size += len(row.raw_text)
            if size >= chunk_bytes or len(chunk) >= SORT_CHUNK_ROWS:
                _write_chunk(writer, chunk, schema, chunk_bytes)
                chunk, size = [], 0
        if chunk:
            _write_chunk(writer, chunk, schema, chunk_bytes)


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
