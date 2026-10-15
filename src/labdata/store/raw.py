"""Raw packing: live results as Parquet rows under ``raw/<source>/<bucket>.parquet``.

Each row is ``accession``, ``raw_text``, ``hash`` and ``fetch_date``. Any change rewrites
the whole bucket file. Writer side only, so pyarrow is imported here freely.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from collections.abc import Iterable
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

_SCHEMA = pa.schema(
    [
        ("accession", pa.string()),
        ("raw_text", pa.string()),
        ("hash", pa.string()),
        ("fetch_date", pa.string()),
    ]
)
_GEO = re.compile(r"^(G(?:SE|SM|PL|DS))(\d+)$")
_OTHER = re.compile(r"^([A-Z]+)(\d+)$")


def sha256(text: str) -> str:
    """Return the hex sha256 of a text's UTF-8 bytes: every ``hash`` and ``content_hash``."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def bucket_of(accession: str) -> str:
    """Return the bucket an accession's raw rows go in.

    GEO accessions use GEO's thousand folder (``GSE246310`` -> ``GSE246nnn``); every other
    prefix uses a range of 10,000 numbers (``SRX24283130`` -> ``SRX2428nnnn``).
    """
    if match := _GEO.match(accession):
        return f"{match[1]}{match[2][:-3]}nnn"
    if match := _OTHER.match(accession):
        return f"{match[1]}{match[2][:-4]}nnnn"
    raise ValueError(f"no raw bucket for {accession!r}")


@dataclass(frozen=True, slots=True)
class RawRow:
    """One raw item: the accession it was fetched for, its text and the fetch date."""

    accession: str
    raw_text: str
    fetch_date: str


def write_rows(root: Path, source: str, rows: list[RawRow]) -> list[Path]:
    """Merge rows into their buckets, replacing rows of the same accession.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    source : str
        The raw folder name, e.g. ``"sra-xml"``.
    rows : list of RawRow
        The new rows. An empty ``raw_text`` is a result too and is kept.

    Returns
    -------
    list of pathlib.Path
        The bucket files rewritten.
    """
    by_bucket: dict[str, list[RawRow]] = defaultdict(list)
    for row in rows:
        by_bucket[bucket_of(row.accession)].append(row)
    folder = root / "raw" / source
    folder.mkdir(parents=True, exist_ok=True)
    written = []
    for bucket, new in sorted(by_bucket.items()):
        path = folder / f"{bucket}.parquet"
        merged = {row.accession: row for row in _read(path)}
        merged.update((row.accession, row) for row in new)
        ordered = [merged[accession] for accession in sorted(merged)]
        table = pa.table(
            {
                "accession": [row.accession for row in ordered],
                "raw_text": [row.raw_text for row in ordered],
                "hash": [sha256(row.raw_text) for row in ordered],
                "fetch_date": [row.fetch_date for row in ordered],
            },
            schema=_SCHEMA,
        )
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp)
        tmp.replace(path)
        written.append(path)
    return written


def saved(root: Path, source: str, accessions: Iterable[str]) -> dict[str, str]:
    """Return the saved raw text of those ``accessions`` that have a row, reading their buckets."""
    wanted = set(accessions)
    folder = root / "raw" / source
    return {
        row.accession: row.raw_text
        for bucket in sorted({bucket_of(a) for a in wanted})
        for row in _read(folder / f"{bucket}.parquet")
        if row.accession in wanted
    }


def read_rows(root: Path, source: str) -> list[RawRow]:
    """Return every raw row of one source, sorted by accession."""
    folder = root / "raw" / source
    rows = [row for path in sorted(folder.glob("*.parquet")) for row in _read(path)]
    return sorted(rows, key=lambda row: row.accession)


def _read(path: Path) -> list[RawRow]:
    if not path.is_file():
        return []
    data = pq.read_table(path).to_pydict()
    return [
        RawRow(accession, text, date)
        for accession, text, date in zip(
            data["accession"], data["raw_text"], data["fetch_date"], strict=True
        )
    ]
