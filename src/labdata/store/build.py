"""Build the store's tables from ``raw/`` and ``snapshots/``, then switch the marker to them.

No network. Reads family SOFT, ``elink`` results, SRA packages and ``[si]`` results from
``raw/``, and the snapshots only through their extracts (``labdata store extract``). Writes ``builds/<build-id>/``: ``records/<type>.parquet`` (sorted by accession),
``links_by_from.parquet``, ``links_by_to.parquet``, ``holdings.parquet``,
``known.parquet``, ``supplementary_files.parquet``, ``gaps.parquet`` and ``notes.parquet``,
with the folder's README and manifest. The folder appears by a rename once whole, and the marker switches to it by a
rename, so readers never see half a build.
"""

from __future__ import annotations

import datetime as dt
import fcntl
import glob
import logging
import re
import shutil
import tomllib
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from labdata.exceptions import StoreBuildError
from labdata.store import extract, gaps, known, provenance, raw, snapshots, sources, suppl, targets
from labdata.store._files import temp_beside, toml_line, write_atomic
from labdata.store.model import RELATIONS
from labdata.store.root import MARKER
from labdata.store.sources import LinkRow, Parsed

#: The marker key naming the current build.
BUILD_KEY = "build"

#: The marker key holding when the current build was switched to, in UTC.
SWITCHED_KEY = "switched"

#: The marker table mapping each earlier build to when it was switched away from, in UTC.
SWITCHED_AWAY_KEY = "switched_away"

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
_NUMBER = re.compile(r"(\d+)$")

logger = logging.getLogger(__name__)


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
        If a snapshot is missing or not extracted, a build of that id exists, two copies of a
        record differ, or another build of this store is running.
    """
    with build_lock(root):
        return _build(root, build_id)


@contextmanager
def build_lock(root: Path) -> Iterator[None]:
    """Hold the store's build lock, ``<root>/work/build.lock``, for one writer at a time.

    Raises
    ------
    StoreBuildError
        If another process or caller holds it.
    """
    lock = root / "work" / "build.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise StoreBuildError(
                f"another build, prune or copy of this store is running (it holds {lock}); "
                "wait for it to finish, then try again"
            ) from None
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _build(root: Path, build_id: str | None) -> Path:
    build_id = build_id or dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    out = root / "builds" / build_id
    if out.exists():
        raise StoreBuildError(f"build {build_id} already exists: {out}")
    for folder in sorted(p for p in (root / "raw").glob("*") if p.is_dir()):
        provenance.document_raw(root, folder.name)

    _, geo_date = extract.done_dir(root, snapshots.GEO_SERIES)
    target_list = targets.target_list(root, geo_date)
    records: dict[str, _Row] = {}
    links: set[LinkRow] = set(targets.summary_links(target_list))
    _geo(root, records, links)
    _sra(root, records, links)
    _sra_typed(root, records)
    _bioprojects(root, records, links)
    _biosamples(root, records, links)
    for row in raw.read_rows(root, raw.PUBMED_SI):
        links.update(sources.pubmed_si(row.accession, row.raw_text))

    for stale in out.parent.glob(f".{glob.escape(build_id)}.*.tmp"):
        shutil.rmtree(stale, ignore_errors=True)
    tmp = temp_beside(out)
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
    known.write(root, tmp / "known.parquet")
    suppl.write(root, tmp / "supplementary_files.parquet")
    labels = _unknown_labels(links)
    coverage = gaps.write_gaps(root, tmp, target_list, geo_date)
    note_counts = gaps.write_notes(
        root,
        tmp,
        target_list,
        [
            gaps.Note(str(v["example"]), gaps.UNKNOWN_LABEL, f"{label}: {v['links']} links")
            for label, v in labels.items()
        ],
    )
    provenance.document_build(
        root,
        tmp,
        build_id,
        unknown_labels=labels,
        target_series=coverage,
        notes=note_counts,
    )
    tmp.rename(out)
    _switch(root, build_id)
    return out


def _unknown_labels(links: set[LinkRow]) -> dict[str, dict[str, int | str]]:
    """Count the links under each GEO relation label not in the fixed list, and log each."""
    found: dict[str, list[str]] = defaultdict(list)
    for link in links:
        if link.source == sources.SOFT and link.relation not in RELATIONS:
            found[link.relation].append(link.from_)
    out: dict[str, dict[str, int | str]] = {}
    for label in sorted(found):
        example = min(found[label])
        out[label] = {"links": len(found[label]), "example": example}
        logger.warning(
            "kept unknown GEO relation label %r verbatim: %d links, e.g. %s",
            label,
            len(found[label]),
            example,
        )
    return out


def _number(accession: str) -> int:
    match = _NUMBER.search(accession)
    return int(match[1]) if match else 0


def _geo(root: Path, records: dict[str, _Row], links: set[LinkRow]) -> None:
    """Add GEO records from family SOFT, a GSM or GPL from its lowest-numbered Series."""
    rows = sorted(raw.read_rows(root, raw.GEO_SOFT), key=lambda r: _number(r.accession))
    for row in rows:
        parsed, stated = sources.soft(row.raw_text)
        links.update(stated)
        for record in parsed:
            held = records.get(record.accession)
            if held is None:
                records[record.accession] = _Row(record, row.fetch_date)
            elif held.parsed.raw_text != record.raw_text:
                raise StoreBuildError(
                    f"copies of {record.accession} differ between family SOFT files "
                    f"(kept the copy in the lowest-numbered Series; {row.accession} differs)"
                )
    for row in raw.read_rows(root, raw.ENTREZ_LINKS):
        links.update(sources.entrez_links(row.raw_text))


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
    _, rows = extract.lookup(root, snapshots.SRA_ACCESSIONS, wanted)
    for accession, status, submitted, updated, published in rows:
        records[str(accession)].parsed.typed = {
            "status": _text(status),
            "submitted": _text(submitted),
            "updated": _text(updated),
            "published": _text(published),
        }


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def _bioprojects(root: Path, records: dict[str, _Row], links: set[LinkRow]) -> None:
    wanted = {link.to for link in links if link.relation == "BioProject"}
    wanted |= {r.accession for r in raw.read_rows(root, raw.PUBMED_SI)}
    wanted = {a for a in wanted if a.startswith("PRJ")}
    if not wanted:
        return
    date, rows = extract.lookup(root, snapshots.BIOPROJECT, wanted)
    for _, text in rows:
        parsed, stated = sources.bioproject(str(text))
        records[parsed.accession] = _Row(parsed, date)
        links.update(stated)


def _biosamples(root: Path, records: dict[str, _Row], links: set[LinkRow]) -> None:
    wanted = {link.to for link in links if link.relation == "BioSample"}
    if not wanted:
        return
    date, rows = extract.lookup(root, snapshots.BIOSAMPLE, wanted)
    for _, text in rows:
        parsed = sources.biosample(str(text))
        records[parsed.accession] = _Row(parsed, date)


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
    """Point the marker at the new build and record when the old one was switched away from."""
    marker = root / MARKER
    with marker.open("rb") as fh:
        settings = tomllib.load(fh)
    now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    old = settings.get(BUILD_KEY)
    if old and old != build_id:
        settings.setdefault(SWITCHED_AWAY_KEY, {})[str(old)] = now
    settings[BUILD_KEY] = build_id
    settings[SWITCHED_KEY] = now
    write_marker(root, settings)


def write_marker(root: Path, settings: dict[str, Any]) -> None:
    """Write the marker whole: its scalar settings, then each table."""
    scalars = [toml_line(k, v) for k, v in settings.items() if not isinstance(v, dict)]
    tables = [
        f"\n[{name}]\n" + "".join(f"{_quoted(k)} = {_quoted(v)}\n" for k, v in rows.items())
        for name, rows in settings.items()
        if isinstance(rows, dict)
    ]
    write_atomic(root / MARKER, "".join(f"{line}\n" for line in scalars) + "".join(tables))


def _quoted(key: str) -> str:
    """Return ``key`` as a quoted TOML key."""
    return toml_line("k", key)[len("k = ") :]
