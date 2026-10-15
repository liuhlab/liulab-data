"""Build ``known.parquet``: every accession the snapshots list, with its snapshot's date.

Each snapshot source is indexed on its own into ``<root>/work/known/<source>-<date>.parquet``,
deduped and sorted. The file appears by an atomic rename, so a killed job leaves none behind
and a rerun skips a source already done. Sources can run as separate jobs; the build only
merges their files. Snapshots are read as streams and DuckDB sorts on disk, so the largest
snapshot never sits in memory.
"""

from __future__ import annotations

import json
import shutil
from itertools import batched
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from labdata.store import snapshots

#: The snapshot sources ``known.parquet`` draws from.
SOURCES = (
    snapshots.GEO_SERIES,
    snapshots.SRA_ACCESSIONS,
    snapshots.BIOPROJECT,
    snapshots.BIOSAMPLE,
)

#: The Parquet key-value metadata key holding each source's snapshot date, as JSON.
DATES_KEY = "snapshot_dates"

_SCHEMA = pa.schema(
    [("accession", pa.string()), ("source", pa.string()), ("snapshot_date", pa.string())]
)
_BATCH = 1 << 20


def work_dir(root: Path) -> Path:
    """Return the folder holding the per-source intermediate files."""
    return root / "work" / "known"


def index_source(root: Path, source: str) -> Path:
    """Index one source's newest snapshot, unless its intermediate file already exists.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    source : str
        One of ``SOURCES``.

    Returns
    -------
    pathlib.Path
        The intermediate file, ``<root>/work/known/<source>-<date>.parquet``.

    Raises
    ------
    ValueError
        If ``source`` is not one of ``SOURCES``.
    StoreBuildError
        If the source has no snapshot.
    """
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}; known: {', '.join(SOURCES)}")
    snap = snapshots.snapshot(root, source)
    out = work_dir(root) / f"{source}-{snap.date}.parquet"
    if out.is_file():
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    parts = out.with_name(f".{out.stem}.parts.tmp")
    tmp = out.with_name(f".{out.stem}.tmp")
    temp = out.with_name(f".{out.stem}.duckdb")
    try:
        with pq.ParquetWriter(parts, _SCHEMA) as writer:
            for batch in batched(snapshots.accessions(snap, source), _BATCH, strict=False):
                n = len(batch)
                columns = {
                    "accession": list(batch),
                    "source": [source] * n,
                    "snapshot_date": [snap.date] * n,
                }
                writer.write_table(pa.table(columns, schema=_SCHEMA))
        _copy(
            f"SELECT DISTINCT accession, source, snapshot_date FROM read_parquet('{_q(parts)}') "
            "ORDER BY accession",
            tmp,
            temp,
        )
        tmp.replace(out)
    finally:
        parts.unlink(missing_ok=True)
        tmp.unlink(missing_ok=True)
        shutil.rmtree(temp, ignore_errors=True)
    return out


def write(root: Path, out: Path) -> dict[str, str]:
    """Write ``out`` by merging each source's intermediate file, indexing any not yet done.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    out : pathlib.Path
        The ``known.parquet`` path.

    Returns
    -------
    dict of str to str
        Each source's snapshot date, also stored in the file's metadata under ``DATES_KEY``.
    """
    files = {source: index_source(root, source) for source in SOURCES}
    dates = {source: path.stem.removeprefix(f"{source}-") for source, path in files.items()}
    meta = _sql(json.dumps(dates, sort_keys=True))
    union = " UNION ALL ".join(f"SELECT * FROM read_parquet('{_q(p)}')" for p in files.values())
    _copy(
        f"SELECT * FROM ({union}) ORDER BY accession, source",
        out,
        out.with_name(f".{out.stem}.duckdb"),
        f", KV_METADATA {{{DATES_KEY}: '{meta}'}}",
    )
    return dates


def _copy(query: str, out: Path, temp: Path, options: str = "") -> None:
    con = duckdb.connect(":memory:", config={"temp_directory": str(temp)})
    try:
        con.execute(f"COPY ({query}) TO '{_q(out)}' (FORMAT parquet{options})")
    finally:
        con.close()
        shutil.rmtree(temp, ignore_errors=True)


def _q(path: Path) -> str:
    return _sql(str(path))


def _sql(text: str) -> str:
    return text.replace("'", "''")
