"""Build the store's tables from ``raw/`` and ``snapshots/``, then switch the marker to them.

No network. Writes ``builds/<build-id>/``: ``records/<type>.parquet`` (sorted by accession),
``links_by_from.parquet``, ``links_by_to.parquet`` and ``holdings.parquet``.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import tomllib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata.exceptions import StoreBuildError
from labdata.store import raw, snapshots, sources
from labdata.store.root import MARKER
from labdata.store.sources import LinkRow, Parsed

#: The marker key naming the current build.
BUILD_KEY = "build"

_FIELDS = pa.list_(pa.struct([("name", pa.string()), ("value", pa.string())]))
_RECORD_SCHEMA = pa.schema(
    [
        ("accession", pa.string()),
        ("type", pa.string()),
        ("status", pa.string()),
        ("submitted", pa.string()),
        ("updated", pa.string()),
        ("published", pa.string()),
        ("fields", _FIELDS),
        ("raw_text", pa.string()),
        ("content_hash", pa.string()),
        ("snapshot_date", pa.string()),
    ]
)
_LINK_SCHEMA = pa.schema(
    [("from", pa.string()), ("to", pa.string()), ("relation", pa.string()), ("source", pa.string())]
)
_BIOPROJECT_KEY = re.compile(r'<ArchiveID\b[^>]*?\baccession="(PRJ[A-Z]+\d+)"')
_BIOSAMPLE_KEY = re.compile(r'<BioSample\b[^>]*?\baccession="(SAM[A-Z]+\d+)"')
_NUMBER = re.compile(r"(\d+)$")


@dataclass(slots=True)
class _Row:
    parsed: Parsed
    snapshot_date: str


def build(root: Path, build_id: str | None = None) -> Path:
    """Build every table into a new ``builds/<build-id>/`` and point the marker at it.

    Parameters
    ----------
    root : pathlib.Path
        The store root, which must carry ``labdata-store.toml``.
    build_id : str or None
        The new build's folder name; defaults to a UTC timestamp.

    Returns
    -------
    pathlib.Path
        The new build folder.

    Raises
    ------
    StoreBuildError
        If a snapshot is missing, a build of that id exists, or two copies of a record differ.
    """
    build_id = build_id or dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    out = root / "builds" / build_id
    if out.exists():
        raise StoreBuildError(f"build {build_id} already exists: {out}")

    records: dict[str, _Row] = {}
    links: set[LinkRow] = set()
    _sra(root, records, links)
    _sra_typed(root, records)
    _bioprojects(root, records, links)
    _biosamples(root, records, links)
    for row in raw.read_rows(root, raw.PUBMED_SI):
        links.update(sources.pubmed_si(row.accession, row.raw_text))

    tmp = root / "builds" / f".{build_id}.tmp"
    (tmp / "records").mkdir(parents=True)
    _write_records(tmp, records)
    _write_links(tmp, links)
    pq.write_table(
        pa.table(
            {
                "accession": sorted(records),
                "type": [records[a].parsed.type for a in sorted(records)],
            }
        ),
        tmp / "holdings.parquet",
    )
    tmp.rename(out)
    _switch(root, build_id)
    return out


def _number(accession: str) -> int:
    match = _NUMBER.search(accession)
    return int(match[1]) if match else 0


def _sra(root: Path, records: dict[str, _Row], links: set[LinkRow]) -> None:
    rows = sorted(raw.read_rows(root, raw.SRA_XML), key=lambda r: _number(r.accession))
    for row in rows:
        parsed, stated = sources.sra_package(row.raw_text)
        links.update(stated)
        for record in parsed:
            held = records.get(record.accession)
            if held is None:
                records[record.accession] = _Row(record, row.fetch_date)
            elif held.parsed.raw_text != record.raw_text:
                raise StoreBuildError(
                    f"copies of {record.accession} differ between experiment packages "
                    f"(kept the copy in the lowest-numbered one; {row.accession} differs)"
                )


def _sra_typed(root: Path, records: dict[str, _Row]) -> None:
    wanted = {a for a, r in records.items() if r.parsed.type.startswith("SRA ")}
    if not wanted:
        return
    tab = snapshots.snapshot(root, snapshots.SRA_ACCESSIONS).path
    for row in snapshots.tab_rows(
        tab,
        lambda r: r.get("Accession") in wanted,
        line_filter=lambda line: line[: line.find("\t")] in wanted,
    ):
        records[row["Accession"]].parsed.typed = {
            "status": row.get("Status") or None,
            "submitted": (row.get("Received") or "")[:10] or None,
            "updated": (row.get("Updated") or "")[:10] or None,
            "published": (row.get("Published") or "")[:10] or None,
        }


def _bioprojects(root: Path, records: dict[str, _Row], links: set[LinkRow]) -> None:
    wanted = {link.to for link in links if link.relation == "BioProject"}
    wanted |= {r.accession for r in raw.read_rows(root, raw.PUBMED_SI)}
    wanted = {a for a in wanted if a.startswith("PRJ")}
    if not wanted:
        return
    snap = snapshots.snapshot(root, snapshots.BIOPROJECT)
    for text in snapshots.elements(snap.path, "Package", wanted, _BIOPROJECT_KEY).values():
        parsed, stated = sources.bioproject(text)
        records[parsed.accession] = _Row(parsed, snap.date)
        links.update(stated)


def _biosamples(root: Path, records: dict[str, _Row], links: set[LinkRow]) -> None:
    wanted = {link.to for link in links if link.relation == "BioSample"}
    if not wanted:
        return
    snap = snapshots.snapshot(root, snapshots.BIOSAMPLE)
    for text in snapshots.elements(snap.path, "BioSample", wanted, _BIOSAMPLE_KEY).values():
        parsed = sources.biosample(text)
        records[parsed.accession] = _Row(parsed, snap.date)


def _write_records(out: Path, records: dict[str, _Row]) -> None:
    by_type: dict[str, list[_Row]] = defaultdict(list)
    for accession in sorted(records):
        by_type[records[accession].parsed.type].append(records[accession])
    for record_type, rows in by_type.items():
        columns: dict[str, list[object]] = {name: [] for name in _RECORD_SCHEMA.names}
        for row in rows:
            p = row.parsed
            columns["accession"].append(p.accession)
            columns["type"].append(p.type)
            for name in ("status", "submitted", "updated", "published"):
                columns[name].append(p.typed.get(name))
            columns["fields"].append([{"name": k, "value": v} for k, v in p.fields])
            columns["raw_text"].append(p.raw_text)
            columns["content_hash"].append(raw.sha256(p.raw_text))
            columns["snapshot_date"].append(row.snapshot_date)
        name = record_type.lower().replace(" ", "-")
        pq.write_table(
            pa.table(columns, schema=_RECORD_SCHEMA), out / "records" / f"{name}.parquet"
        )


def _write_links(out: Path, links: set[LinkRow]) -> None:
    for name, key in (
        ("links_by_from", lambda r: (r.from_, r.to, r.relation, r.source)),
        ("links_by_to", lambda r: (r.to, r.from_, r.relation, r.source)),
    ):
        rows = sorted(links, key=key)
        table = pa.table(
            {
                "from": [r.from_ for r in rows],
                "to": [r.to for r in rows],
                "relation": [r.relation for r in rows],
                "source": [r.source for r in rows],
            },
            schema=_LINK_SCHEMA,
        )
        pq.write_table(table, out / f"{name}.parquet")


def _switch(root: Path, build_id: str) -> None:
    """Rewrite the marker to name the new build: a temporary file renamed over the old one."""
    marker = root / MARKER
    with marker.open("rb") as fh:
        settings = tomllib.load(fh)
    settings[BUILD_KEY] = build_id
    lines = [f"{key} = {json.dumps(value)}" for key, value in settings.items()]
    tmp = marker.with_name(f".{MARKER}.tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    tmp.replace(marker)
