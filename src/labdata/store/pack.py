"""Pack: merge parts into ``raw/<source>/<bucket>.parquet``, the only writer of ``raw/``.

A bucket is rewritten whole by a k-way merge of its sorted inputs, read a few rows at a
time and written a chunk at a time, so memory holds a chunk, never a bucket or a source.
For one accession, a part's row replaces the bucket's, and a newer part's an older one's.
A bucket file named for another width is first split into parts, so a width change
repacks the same rows.
"""

from __future__ import annotations

import heapq
import itertools
import os
from collections import defaultdict
from collections.abc import Iterable, Iterator
from pathlib import Path

import pyarrow.parquet as pq

from labdata.store import provenance, raw
from labdata.store.raw import RawRow

#: Rows read from each input at a time.
READ_ROWS = 16
#: Most sorted runs merged in one pass; more are merged in passes through spill files.
FAN_IN = 8


def pack(
    root: Path, sources: Iterable[str] | None = None, *, chunk_bytes: int = raw.CHUNK_BYTES
) -> list[Path]:
    """Merge every source's parts into its raw buckets and document the folders changed.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    sources : iterable of str or None
        The raw folders to pack; defaults to every source with parts or a bucket file.
    chunk_bytes : int
        Raw text held before a row group is written.

    Returns
    -------
    list of pathlib.Path
        The bucket files rewritten.
    """
    if sources is None:
        found = {p.name for p in (root / "work" / "parts").glob("*") if p.is_dir()}
        found |= {p.name for p in (root / "raw").glob("*") if p.is_dir()}
        sources = sorted(found)
    written: list[Path] = []
    for source in sources:
        done = _pack_source(root, source, chunk_bytes)
        written += done
        if done:
            provenance.document_raw(root, source)
    return written


def write_rows(root: Path, source: str, rows: list[RawRow]) -> list[Path]:
    """Save rows through parts and pack them: a row replaces any row of its accession.

    Returns
    -------
    list of pathlib.Path
        The bucket files rewritten.
    """
    by_bucket: dict[str, list[RawRow]] = defaultdict(list)
    for row in rows:
        by_bucket[raw.bucket_of(source, row.accession)].append(row)
    for bucket, chunk in sorted(by_bucket.items()):
        piece = raw.Piece(source, bucket, tuple(sorted({row.accession for row in chunk})))
        raw.write_part(root, piece, chunk)
    return pack(root, [source])


def _pack_source(root: Path, source: str, chunk_bytes: int) -> list[Path]:
    folder = root / "raw" / source
    for stale in [*folder.glob(".*.tmp"), *raw.parts_dir(root, source).glob(".*")]:
        stale.unlink(missing_ok=True)
    for path in sorted(folder.glob("*.parquet")):
        _split_if_misnamed(root, source, path, chunk_bytes)
    by_bucket: dict[str, list[Path]] = defaultdict(list)
    for part in raw.part_files(root, source):
        by_bucket[raw.part_bucket(part)].append(part)
    written = []
    for bucket, parts in sorted(by_bucket.items()):
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{bucket}.parquet"
        runs = list(_runs(parts))
        if path.is_file():
            runs.insert(0, _rows(path))
        spills: list[Path] = []
        runs = _fewer(runs, raw.parts_dir(root, source), spills, chunk_bytes)
        raw.write_sorted_runs(path, _merged(runs), raw.SCHEMA, chunk_bytes=chunk_bytes)
        for done in [*parts, *spills]:
            done.unlink()
        written.append(path)
    return written


def _fewer(
    runs: list[Iterator[RawRow]], folder: Path, spills: list[Path], chunk_bytes: int
) -> list[Iterator[RawRow]]:
    """Merge neighbouring runs into spill files until at most :data:`FAN_IN` are left.

    Merging neighbours keeps the runs' order, so the newer row still wins.
    """
    while len(runs) > FAN_IN:
        fewer = []
        for start in range(0, len(runs), FAN_IN):
            group = runs[start : start + FAN_IN]
            if len(group) == 1:
                fewer += group
                continue
            spill = folder / f".spill-{len(spills)}.parquet"
            raw.write_sorted_runs(spill, _merged(group), raw.SCHEMA, chunk_bytes=chunk_bytes)
            spills.append(spill)
            fewer.append(_rows(spill))
        runs = fewer
    return runs


def _split_if_misnamed(root: Path, source: str, path: Path, chunk_bytes: int) -> None:
    """Move a bucket file's rows into parts when its name is not their bucket, then drop it.

    The parts are named for the old file and dated as old as possible, so a part fetched
    since still wins.
    """
    accessions = pq.read_table(path, columns=["accession"]).column(0).to_pylist()
    if all(raw.bucket_of(source, a) == path.stem for a in accessions):
        return
    by_bucket: dict[str, list[str]] = defaultdict(list)
    for accession in accessions:
        by_bucket[raw.bucket_of(source, accession)].append(accession)
    folder = raw.parts_dir(root, source)
    folder.mkdir(parents=True, exist_ok=True)
    for bucket, members in sorted(by_bucket.items()):
        wanted = set(members)
        part = folder / f"{bucket}.repack-{path.stem}.parquet"
        rows = (row for row in _rows(path) if row.accession in wanted)
        raw.write_sorted_runs(part, rows, raw.SCHEMA, chunk_bytes=chunk_bytes)
        _age(part)
    path.unlink()


def _age(path: Path) -> None:
    os.utime(path, ns=(0, 0))


def _rows(path: Path, groups: list[int] | None = None) -> Iterator[RawRow]:
    """Yield a file's rows, or some row groups', in file order, one row group at a time."""
    with pq.ParquetFile(path, pre_buffer=False) as fh:
        for group in range(fh.num_row_groups) if groups is None else groups:
            for batch in fh.iter_batches(
                batch_size=READ_ROWS,
                row_groups=[group],
                columns=["accession", "raw_text", "fetch_date"],
            ):
                data = batch.to_pydict()
                yield from map(RawRow, data["accession"], data["raw_text"], data["fetch_date"])


def _runs(paths: list[Path]) -> Iterator[Iterator[RawRow]]:
    """Yield each sorted run of the files, oldest first: row groups in order, by their stats."""
    for path in paths:
        metadata = pq.read_metadata(path)
        run: list[int] = []
        last: str | None = None
        for group in range(metadata.num_row_groups):
            stats = metadata.row_group(group).column(0).statistics
            low, high = (stats.min, stats.max) if stats and stats.has_min_max else (None, None)
            if run and (low is None or last is None or low < last):
                yield _rows(path, run)
                run = []
            run.append(group)
            last = high
        if run:
            yield _rows(path, run)


def _merged(runs: list[Iterator[RawRow]]) -> Iterator[RawRow]:
    """Merge sorted runs into one sorted stream, keeping the last run's row per accession."""
    streams = [_ranked(rank, run) for rank, run in enumerate(runs)]
    merged = heapq.merge(*streams, key=lambda item: (item[0], item[1]))
    for _, group in itertools.groupby(merged, key=lambda item: item[0]):
        *_, (_, _, row) = group
        yield row


def _ranked(rank: int, run: Iterator[RawRow]) -> Iterator[tuple[str, int, RawRow]]:
    return ((row.accession, rank, row) for row in run)
