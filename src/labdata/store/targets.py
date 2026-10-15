"""The target Series list: the sequencing Series of one dated ``geo-series`` snapshot.

A Series is a target when its ``esummary`` ``gdstype`` names high-throughput sequencing,
whatever its SOFT ``!Series_type`` says. The list is streamed from the snapshot into
``<root>/work/targets/geo-series-<date>.parquet`` once per snapshot date; a build or a
coverage check remakes it when missing.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata.store import snapshots
from labdata.store._files import temp_beside
from labdata.store.sources import GEO_SUMMARY, LinkRow

#: The ``gdstype`` text that puts a Series on the target list, matched case-insensitively.
SEQUENCING = "high throughput sequencing"

SCHEMA = pa.schema(
    [("accession", pa.string()), ("gdstype", pa.string()), ("bioproject", pa.string())]
)
_BATCH = 10_000


def is_target(gdstype: str) -> bool:
    """Return whether an ``esummary`` ``gdstype`` puts a Series on the target list."""
    return SEQUENCING in gdstype.lower()


def target_list(root: Path, date: str | None = None) -> Path:
    """Return the target Series list of one ``geo-series`` snapshot, writing it if missing.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    date : str or None
        The snapshot date; defaults to the newest snapshot.

    Returns
    -------
    pathlib.Path
        A Parquet file of ``accession``, ``gdstype`` and ``bioproject`` (empty when the
        snapshot gives none), one row per target Series, in snapshot order.

    Raises
    ------
    StoreBuildError
        If the store has no such snapshot.
    """
    snap = snapshots.snapshot(root, snapshots.GEO_SERIES, date)
    out = root / "work" / "targets" / f"geo-series-{snap.date}.parquet"
    if out.is_file():
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = temp_beside(out)
    try:
        with pq.ParquetWriter(tmp, SCHEMA) as writer:
            batch: list[tuple[str, str, str]] = []
            for row in _targets(snap.path):
                batch.append(row)
                if len(batch) == _BATCH:
                    writer.write_table(_table(batch))
                    batch = []
            writer.write_table(_table(batch))
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


def summary_links(path: Path) -> Iterator[LinkRow]:
    """Yield each target Series' snapshot BioProject as a ``GEO summary`` link, batch by batch.

    Parameters
    ----------
    path : pathlib.Path
        A target Series list from :func:`target_list`.
    """
    for batch in pq.ParquetFile(path).iter_batches(columns=["accession", "bioproject"]):
        for gse, prj in zip(*batch.to_pydict().values(), strict=True):
            if prj:
                yield LinkRow(gse, prj, "BioProject", GEO_SUMMARY)


def _targets(path: Path) -> Iterator[tuple[str, str, str]]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            record = json.loads(line)
            kind = record.get("gdstype") or ""
            if is_target(kind):
                yield record["accession"], kind, (record.get("bioproject") or "").strip()


def _table(rows: list[tuple[str, str, str]]) -> pa.Table:
    return pa.table(
        {name: [row[i] for row in rows] for i, name in enumerate(SCHEMA.names)}, schema=SCHEMA
    )
