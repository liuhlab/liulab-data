"""The target Series list: the sequencing Series of one dated ``geo-series`` snapshot.

A Series is a target when its ``esummary`` ``gdstype`` names high-throughput sequencing,
whatever its SOFT ``!Series_type`` says. The list is streamed from the snapshot into
``<root>/work/targets/geo-series-<date>.parquet`` once per snapshot date; a build or a
coverage check remakes it when missing.

A round of the full run works through a seeded sample of the list. A Series is in the sample
of fraction ``f`` when its seeded hash falls below ``f``, so a smaller round's sample lies
inside every larger one's, whatever the list's order or length.
"""

from __future__ import annotations

import gzip
import hashlib
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

#: The seed of every round's sample; changing it would break nesting with earlier rounds.
SEED = "labdata-round"


def in_sample(accession: str, fraction: float) -> bool:
    """Return whether a Series is in the seeded sample of ``fraction`` of the target list.

    Examples
    --------
    >>> in_sample("GSE1", 1.0)
    True
    """
    digest = hashlib.blake2b(f"{SEED}:{accession}".encode(), digest_size=8).digest()
    return int.from_bytes(digest) < fraction * 2**64


def check_fraction(fraction: float | None) -> float | None:
    """Return ``fraction``, or ``None`` for the whole list (``None`` or ``1``).

    Raises
    ------
    ValueError
        If the fraction is not in (0, 1].
    """
    if fraction is None or fraction == 1:
        return None
    if not 0 < fraction < 1:
        raise ValueError(f"a round's fraction must be in (0, 1], not {fraction}")
    return fraction


def fraction_name(fraction: float | None) -> str:
    """Return the file-name tag of a round's sample: ``all``, or e.g. ``f0.01``."""
    fraction = check_fraction(fraction)
    return "all" if fraction is None else f"f{fraction:g}"


def is_target(gdstype: str) -> bool:
    """Return whether an ``esummary`` ``gdstype`` puts a Series on the target list."""
    return SEQUENCING in gdstype.lower()


def list_path(root: Path, date: str, fraction: float | None = None) -> Path:
    """Return where :func:`target_list` writes the list of one snapshot date and fraction."""
    fraction = check_fraction(fraction)
    tag = "" if fraction is None else f"-{fraction_name(fraction)}"
    return root / "work" / "targets" / f"geo-series-{date}{tag}.parquet"


def target_list(root: Path, date: str | None = None, fraction: float | None = None) -> Path:
    """Return the target Series list of one ``geo-series`` snapshot, writing it if missing.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    date : str or None
        The snapshot date; defaults to the newest snapshot.
    fraction : float or None
        A round's seeded sample of the list (see :func:`in_sample`); ``None`` or ``1`` for all.

    Returns
    -------
    pathlib.Path
        A Parquet file of ``accession``, ``gdstype`` and ``bioproject`` (empty when the
        snapshot gives none), one row per target Series, in snapshot order.

    Raises
    ------
    StoreBuildError
        If the store has no such snapshot.
    ValueError
        If the fraction is not in (0, 1].
    """
    fraction = check_fraction(fraction)
    snap = snapshots.snapshot(root, snapshots.GEO_SERIES, date)
    out = list_path(root, snap.date, fraction)
    if out.is_file():
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = temp_beside(out)
    try:
        with pq.ParquetWriter(tmp, SCHEMA) as writer:
            batch: list[tuple[str, str, str]] = []
            for row in _targets(snap.path):
                if fraction is not None and not in_sample(row[0], fraction):
                    continue
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
