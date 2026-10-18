"""Parse raw rows and snapshot texts into record, link and file-list parts for the build.

A task reads some row groups of some Parquet files in small batches, and writes its
parts a chunk at a time: a worker holds about one chunk of text, never a whole file.
Parts land under the build's work folder as ``records/<type>/<task>.parquet``,
``links/<task>.parquet`` and ``filelist/<task>.parquet``.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata.exceptions import StoreBuildError
from labdata.store import raw, snapshots, sources, suppl
from labdata.store.layout import record_stem

_FIELDS = pa.list_(pa.struct([("name", pa.string()), ("value", pa.string())]))

#: A record part: the build's record columns, then the copy's ``origin`` (the raw row it was
#: parsed from), that origin's accession ``number`` and ``seq``, unique across the build.
RECORD_SCHEMA = pa.schema(
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
        ("origin", pa.string()),
        ("number", pa.int64()),
        ("seq", pa.int64()),
    ]
)
LINK_SCHEMA = pa.schema(
    [("from", pa.string()), ("to", pa.string()), ("relation", pa.string()), ("source", pa.string())]
)
#: A ``filelist.txt`` entry with the Series that lists it and that Series' number.
FILELIST_SCHEMA = pa.schema(
    [
        ("series", pa.string()),
        ("number", pa.int64()),
        ("name", pa.string()),
        ("size", pa.int64()),
        ("date", pa.string()),
        ("type", pa.string()),
        ("archive", pa.string()),
    ]
)

#: The raw sources the build parses.
RAW_KINDS = (raw.GEO_SOFT, raw.SRA_XML, raw.ENTREZ_LINKS, raw.PUBMED_SI, raw.GEO_FILELIST)
#: The snapshot sources whose wanted records the build parses.
SNAPSHOT_KINDS = (snapshots.BIOPROJECT, snapshots.BIOSAMPLE)

_BATCH_ROWS = 16
_NUMBER = re.compile(r"(\d+)$")


def number(accession: str) -> int:
    """Return an accession's trailing number; the lowest-numbered copy of a record is kept."""
    match = _NUMBER.search(accession)
    return int(match[1]) if match else 0


@dataclass(frozen=True, slots=True)
class Task:
    """Some row groups of some files of one kind to parse.

    ``inputs`` pairs each file with the row groups to read. ``date`` is a snapshot's date, used
    in place of each raw row's fetch date.
    """

    kind: str
    inputs: tuple[tuple[Path, tuple[int, ...]], ...]
    out: Path
    index: int
    chunk_bytes: int
    date: str | None = None


@dataclass(slots=True)
class _Parts:
    task: Task
    records: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    links: list[dict[str, object]] = field(default_factory=list)
    listed: list[dict[str, object]] = field(default_factory=list)
    writers: dict[str, pq.ParquetWriter] = field(default_factory=dict)
    seq: int = 0

    def record(self, parsed: sources.Parsed, date: str, origin: str, num: int) -> None:
        self.seq += 1
        self.records.setdefault(parsed.type, []).append(
            {
                "accession": parsed.accession,
                "type": parsed.type,
                **{k: parsed.typed.get(k) for k in ("status", "submitted", "updated", "published")},
                "fields": [{"name": k, "value": v} for k, v in parsed.fields],
                "raw_text": parsed.raw_text,
                "content_hash": raw.sha256(parsed.raw_text),
                "snapshot_date": date,
                "origin": origin,
                "number": num,
                "seq": (self.task.index << 32) | self.seq,
            }
        )

    def link(self, rows: list[sources.LinkRow]) -> None:
        self.links.extend(
            {"from": r.from_, "to": r.to, "relation": r.relation, "source": r.source} for r in rows
        )

    def flush(self) -> None:
        for record_type, rows in self.records.items():
            self._write(f"records/{record_stem(record_type)}", rows, RECORD_SCHEMA)
        self._write("links", self.links, LINK_SCHEMA)
        self._write("filelist", self.listed, FILELIST_SCHEMA)
        self.records, self.links, self.listed = {}, [], []

    def close(self) -> None:
        self.flush()
        for writer in self.writers.values():
            writer.close()

    def _write(self, folder: str, rows: list[dict[str, object]], schema: pa.Schema) -> None:
        if not rows:
            return
        if folder not in self.writers:
            path = self.task.out / folder / f"{self.task.index:06d}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            self.writers[folder] = pq.ParquetWriter(path, schema)
        self.writers[folder].write_table(pa.Table.from_pylist(rows, schema=schema))


@dataclass(slots=True)
class Left:
    """The SRA experiments a task made no records for.

    Attributes
    ----------
    empty : list of str
        Experiments whose saved package is empty: NCBI served none.
    unparsable : list of tuple of str
        Experiments whose saved package is not well-formed XML, each with the parse error.
    """

    empty: list[str] = field(default_factory=list)
    unparsable: list[tuple[str, str]] = field(default_factory=list)


def run(task: Task) -> Left:
    """Parse one task's rows into parts.

    Returns
    -------
    Left
        The SRA experiments the task made no records for.

    Raises
    ------
    StoreBuildError
        If any other row fails to parse; the message names the row and the reason.
    """
    parts = _Parts(task)
    left = Left()
    snapshot = task.date is not None
    columns = ["accession", "raw_text"] + ([] if snapshot else ["fetch_date"])
    held = 0
    try:
        for path, groups in task.inputs:
            with pq.ParquetFile(path, buffer_size=1 << 20, pre_buffer=False) as source:
                for batch in source.iter_batches(
                    batch_size=_BATCH_ROWS, row_groups=list(groups), columns=columns
                ):
                    data = batch.to_pydict()
                    dates = data.get("fetch_date") or [task.date] * batch.num_rows
                    for accession, text, date in zip(
                        data["accession"], data["raw_text"], dates, strict=True
                    ):
                        held += len(text)
                        if task.kind == raw.SRA_XML and not text:
                            left.empty.append(accession)
                            continue
                        try:
                            _parse(parts, task.kind, accession, text, date)
                        except ET.ParseError as err:
                            if task.kind != raw.SRA_XML:
                                raise _failed(task.kind, accession, err) from None
                            left.unparsable.append((accession, str(err)))
                        except Exception as err:
                            # Ray loses a parse error's message; this one carries it as text.
                            raise _failed(task.kind, accession, err) from None
                    if held >= task.chunk_bytes:
                        parts.flush()
                        held = 0
    finally:
        parts.close()
    return left


def _failed(kind: str, accession: str, err: Exception) -> StoreBuildError:
    return StoreBuildError(f"{kind} {accession} did not parse: {type(err).__name__}: {err}")


def _parse(parts: _Parts, kind: str, accession: str, text: str, date: str) -> None:
    num = number(accession)
    if kind == raw.GEO_SOFT:
        records, links = sources.soft(text)
    elif kind == raw.SRA_XML:
        records, links = sources.sra_package(text)
    elif kind == raw.ENTREZ_LINKS:
        records, links = [], sources.entrez_links(text)
    elif kind == raw.PUBMED_SI:
        records, links = [], sources.pubmed_si(accession, text)
    elif kind == raw.GEO_FILELIST:
        records, links = [], []
        for name, (size, when, file_type, archive) in suppl.filelist(text).items():
            parts.listed.append(
                {
                    "series": accession,
                    "number": num,
                    "name": name,
                    "size": size,
                    "date": when,
                    "type": file_type,
                    "archive": archive,
                }
            )
    elif kind == snapshots.BIOPROJECT:
        record, links = sources.bioproject(text)
        records = [record]
    elif kind == snapshots.BIOSAMPLE:
        records, links = [sources.biosample(text)], []
    else:
        raise ValueError(f"no parser for {kind!r}")
    parts.link(links)
    for record in records:
        if kind in SNAPSHOT_KINDS:
            parts.record(record, date, raw.sha256(text), 0)
        else:
            parts.record(record, date, accession, num)
