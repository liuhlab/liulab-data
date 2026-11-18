"""Pack: merge parts into ``raw/<source>/<bucket>.parquet``, the only writer of ``raw/``.

A bucket is rewritten whole by :func:`labdata.store.raw.newest_rows`, the merge every read
of ``raw/`` uses, its inputs read a few rows at a time and written a chunk at a time, so memory
holds a chunk, never a bucket or a source. A bucket left with no rows is deleted. A bucket
file named for another width is first split into parts, so a width change repacks the same
rows.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections import defaultdict
from collections.abc import Iterable, Iterator
from pathlib import Path

import pyarrow.parquet as pq

from labdata.store import paths, provenance, raw
from labdata.store.raw import RawRow

#: Most sorted runs merged in one pass; more are merged in passes through spill files.
FAN_IN = 8

logger = logging.getLogger(__name__)


def pack(
    root: Path,
    sources: Iterable[str] | None = None,
    *,
    chunk_bytes: int = raw.SORT_CHUNK_BYTES,
    counts: Path | None = None,
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
    counts : pathlib.Path or None
        A file to append one JSON line to per bucket rewritten: its source, bucket, and the
        records added, replaced (held with another text) and removed.

    Returns
    -------
    list of pathlib.Path
        The bucket files rewritten or deleted. Each source's count of removed records is logged.
    """
    if sources is None:
        found = {p.name for p in paths.PARTS.at(root).glob("*") if p.is_dir()}
        found |= {p.name for p in paths.RAW.at(root).glob("*") if p.is_dir()}
        sources = sorted(found)
    written: list[Path] = []
    for source in sources:
        done, removed = _pack_source(root, source, chunk_bytes, counts)
        written += done
        if removed:
            logger.info("%s: removed %d record%s", source, removed, "" if removed == 1 else "s")
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
    raw.save(root, source, rows)
    return pack(root, [source])


def _pack_source(
    root: Path, source: str, chunk_bytes: int, counts: Path | None
) -> tuple[list[Path], int]:
    folder = paths.RAW.at(root, source)
    for stale in [*folder.glob(".*.tmp"), *raw.parts_dir(root, source).glob(".*")]:
        stale.unlink(missing_ok=True)
    for path in sorted(folder.glob("*.parquet")):
        _split_if_misnamed(root, source, path, chunk_bytes)
    written = []
    removed = 0
    for bucket, parts in sorted(raw.parts_by_bucket(root, source).items()):
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{bucket}.parquet"
        spills: list[Path] = []
        runs, bucket_runs = raw.layers(root, source, bucket, parts)
        old, new = runs[:bucket_runs], runs[bucket_runs:]
        # The old bucket is never spilled, so the merge can tell its rows from the parts'.
        newer = _fewer(new, raw.parts_dir(root, source), spills, chunk_bytes, FAN_IN - bucket_runs)
        tally: dict[str, int] = dict.fromkeys(COUNTS, 0)
        rows = _merged([*old, *newer], tally, bucket_runs)
        if counts is not None:
            record = {"source": source, "bucket": bucket, "parts": _parts_tag(parts)}
            rows = _counted(rows, counts, record, tally)
        raw.write_sorted_runs(path, rows, raw.SCHEMA, chunk_bytes=chunk_bytes)
        if pq.read_metadata(path).num_rows == 0:
            path.unlink()
        for done in [*parts, *spills]:
            done.unlink()
        written.append(path)
        removed += tally["removed"]
    return written, removed


#: The counts a pack records per bucket rewritten.
COUNTS = ("added", "replaced", "removed")


def read_counts(path: Path) -> dict[str, dict[str, int]]:
    """Return the records added, replaced and removed per source, from a pack's counts file.

    A bucket packed again from the same parts, after a kill, counts once: its first record.
    """
    out: dict[str, dict[str, int]] = defaultdict(lambda: dict.fromkeys(COUNTS, 0))
    seen: set[tuple[str, str, str]] = set()
    lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:  # a line cut off by a kill
            continue
        key = (row["source"], row["bucket"], row.get("parts", ""))
        if key in seen:
            continue
        seen.add(key)
        for name in COUNTS:
            out[row["source"]][name] += row[name]
    return dict(sorted(out.items()))


def _parts_tag(parts: list[Path]) -> str:
    return hashlib.sha256("\n".join(p.name for p in parts).encode()).hexdigest()[:12]


def _counted(
    rows: Iterator[RawRow], counts: Path, record: dict[str, str], tally: dict[str, int]
) -> Iterator[RawRow]:
    """Pass ``rows`` on, then append the bucket's counts, before its new file is renamed in."""
    yield from rows
    with counts.open("a+b") as fh:
        torn = fh.tell() > 0 and fh.seek(-1, os.SEEK_END) >= 0 and fh.read(1) != b"\n"
        fh.write(b"\n" * torn + json.dumps({**record, **tally}).encode() + b"\n")


def _fewer(
    runs: list[raw.Run],
    folder: Path,
    spills: list[Path],
    chunk_bytes: int,
    most: int = FAN_IN,
) -> list[raw.Run]:
    """Merge neighbouring runs into spill files until at most ``most`` are left.

    Merging neighbours keeps the runs' order, so the newer row still wins. A spill keeps a
    removal as a row dated :data:`labdata.store.raw.REMOVED`.
    """
    while len(runs) > max(most, 1):
        fewer = []
        for start in range(0, len(runs), FAN_IN):
            group = runs[start : start + FAN_IN]
            if len(group) == 1:
                fewer += group
                continue
            spill = folder / f".spill-{len(spills)}.parquet"
            rows = _merged(group, None, 0)
            raw.write_sorted_runs(spill, rows, raw.SCHEMA, chunk_bytes=chunk_bytes)
            spills.append(spill)
            fewer.append(raw.file_run(spill))
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
        part = folder / raw.part_name(bucket, f"repack-{path.stem}")
        rows = (
            row
            for accession, row, _ in raw.newest_rows([raw.file_run(path)])
            if accession in wanted and row is not None
        )
        raw.write_sorted_runs(part, rows, raw.SCHEMA, chunk_bytes=chunk_bytes)
        _age(part)
    path.unlink()


def _age(path: Path) -> None:
    os.utime(path, ns=(0, 0))


def _merged(
    runs: list[raw.Run], tally: dict[str, int] | None, bucket_runs: int
) -> Iterator[RawRow]:
    """Merge sorted runs into one sorted stream of each accession's newest row.

    With ``tally``, the stream is final: a removed accession is dropped, and ``tally`` counts
    the accessions added, replaced with another text and removed against the first
    ``bucket_runs`` runs, the old bucket's. Without, a removal is kept, so a spill file passes
    it on.
    """
    for accession, row, old in raw.newest_rows(runs, bucket_runs):
        if row is not None:
            yield row
        elif tally is None:
            yield RawRow(accession, "", raw.REMOVED)
        if tally is None:
            continue
        if row is None:
            tally["removed"] += old is not None
        elif old is None:
            tally["added"] += 1
        elif old != row.raw_text:
            tally["replaced"] += 1
