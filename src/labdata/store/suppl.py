"""Build ``supplementary_files.parquet`` from family SOFT and each Series' ``filelist.txt``.

Names and URLs come from SOFT ``!…_supplementary_file`` lines. Sizes, dates, types and
archive contents come from ``filelist.txt``, where each ``File`` row belongs to the
``Archive`` row above it. A file several Series list takes its facts from the lowest-numbered
Series. No network.
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata.store import raw, sources

_SCHEMA = pa.schema(
    [
        ("owner", pa.string()),
        ("name", pa.string()),
        ("url", pa.string()),
        ("size", pa.int64()),
        ("date", pa.string()),
        ("type", pa.string()),
        ("archive", pa.string()),
    ]
)
_SUPPL_FIELD = re.compile(r"^(?:Series|Sample|Platform)_supplementary_file(?:_\d+)?$")
_NUMBER = re.compile(r"(\d+)$")

#: One ``filelist.txt`` entry: size, date, type and the archive holding it.
Listed = tuple[int, str, str, str | None]


def filelist(text: str) -> dict[str, Listed]:
    """Parse one ``filelist.txt`` into each file name's size, date, type and archive."""
    out: dict[str, Listed] = {}
    archive: str | None = None
    for line in text.splitlines():
        cells = line.split("\t")
        if line.startswith("#") or len(cells) < 5:
            continue
        kind, name, time, size, file_type = cells[:5]
        date = dt.datetime.strptime(time, "%m/%d/%Y %H:%M:%S").isoformat()
        if kind == "Archive":
            out[name] = (int(size), date, file_type, None)
            archive = name
        else:
            out[name] = (int(size), date, file_type, archive)
    return out


def rows(root: Path) -> list[dict[str, object]]:
    """Return the table's rows from ``raw/``, sorted by owner, then name and URL."""
    soft = sorted(raw.read_rows(root, raw.GEO_SOFT), key=lambda r: _number(r.accession))
    listed: dict[str, tuple[str, Listed]] = {}
    for row in sorted(raw.read_rows(root, raw.GEO_FILELIST), key=lambda r: _number(r.accession)):
        for name, facts in filelist(row.raw_text).items():
            listed.setdefault(name, (row.accession, facts))

    named: dict[tuple[str, str], str] = {}
    for row in soft:
        for _, accession, block in sources.soft_blocks(row.raw_text):
            for name, value in sources.soft_fields(block):
                url = value.strip()
                if _SUPPL_FIELD.match(name) and url and url != "NONE":
                    named.setdefault((accession, url), url.rsplit("/", 1)[-1])

    out: list[dict[str, object]] = []
    for (owner, url), name in named.items():
        out.append(_row(owner, name, url, listed.get(name, ("", None))[1]))
    seen = set(named.values())
    for name, (series, facts) in listed.items():
        if name not in seen:
            out.append(_row(series, name, None, facts))
    return sorted(out, key=lambda r: (r["owner"], r["name"], r["url"] or ""))


def write(root: Path, out: Path) -> None:
    """Write ``supplementary_files.parquet``, sorted by owner accession."""
    pq.write_table(pa.Table.from_pylist(rows(root), schema=_SCHEMA), out)


def _row(owner: str, name: str, url: str | None, facts: Listed | None) -> dict[str, object]:
    size, date, file_type, archive = facts or (None, None, None, None)
    return {
        "owner": owner,
        "name": name,
        "url": url,
        "size": size,
        "date": date,
        "type": file_type,
        "archive": archive,
    }


def _number(accession: str) -> int:
    match = _NUMBER.search(accession)
    return int(match[1]) if match else 0
