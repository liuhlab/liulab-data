"""Extract each bulk snapshot once, in parallel, into Parquet parts the build reads.

One pass per snapshot writes ``<root>/work/extract/<source>-<date>/part-NNNNN.parquet``, one
part per byte range of the (decompressed) snapshot. A worker seeks to its range, skips to the
first record that starts inside it, and stops at the first record that starts at or after the
range's end, so each record lands in exactly one part. Records are found by byte scanning.

Every step resumes: a gzip snapshot is decompressed once into ``<root>/work/tmp/``, each part
appears by a rename and is skipped when present, and ``_DONE`` marks the source finished. The
range plan is saved before the first part, so a rerun splits the file the same way.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from labdata.exceptions import StoreBuildError
from labdata.store import snapshots

#: The snapshot sources an extract covers.
SOURCES = (
    snapshots.BIOSAMPLE,
    snapshots.BIOPROJECT,
    snapshots.SRA_ACCESSIONS,
    snapshots.GEO_SERIES,
)

#: The file that marks a source's extract finished.
DONE = "_DONE"

#: The default size of one part's byte range.
PART_BYTES = 1 << 28

#: The DuckDB memory cap; ``LABDATA_DUCKDB_MEMORY`` overrides it.
DUCKDB_MEMORY = "8GB"

_PLAN = "_plan.json"
_CHUNK = 1 << 24
_BIOPROJECT_KEY = re.compile(rb'<ArchiveID\b[^>]*?\baccession="(PRJ[A-Z]+\d+)"')
_BIOSAMPLE_KEY = re.compile(rb'<BioSample\b[^>]*?\baccession="(SAM[A-Z]+\d+)"')

_TEXT_SCHEMA = pa.schema([("accession", pa.string()), ("raw_text", pa.string())])
_TAB_SCHEMA = pa.schema(
    [
        ("accession", pa.string()),
        ("status", pa.string()),
        ("submitted", pa.string()),
        ("updated", pa.string()),
        ("published", pa.string()),
    ]
)
_GEO_SCHEMA = pa.schema([("accession", pa.string())])


@dataclass(frozen=True, slots=True)
class Task:
    """One part to write: a byte range of the plain snapshot file."""

    source: str
    path: Path
    index: int
    start: int
    end: int
    out: Path


def default_workers() -> int:
    """Return ``SLURM_CPUS_ON_NODE`` if set, else the machine's CPU count."""
    return int(os.environ.get("SLURM_CPUS_ON_NODE") or os.cpu_count() or 1)


def extract_dir(root: Path, source: str, date: str) -> Path:
    """Return the folder holding one snapshot's parts."""
    return root / "work" / "extract" / f"{source}-{date}"


def extract(
    root: Path, source: str, *, workers: int | None = None, part_bytes: int = PART_BYTES
) -> Path:
    """Extract one source's newest snapshot into Parquet parts, resuming any earlier run.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    source : str
        One of ``SOURCES``.
    workers : int or None
        Parallel workers; defaults to :func:`default_workers`. ``1`` runs without Ray.
    part_bytes : int
        The byte range of one part; a resumed run keeps the size its first run saved.

    Returns
    -------
    pathlib.Path
        The extract folder, ``<root>/work/extract/<source>-<date>/``, holding ``_DONE``.

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
    out = extract_dir(root, source, snap.date)
    if (out / DONE).is_file():
        return out
    workers = workers or default_workers()
    out.mkdir(parents=True, exist_ok=True)
    path = _plain(root, snap, workers)
    size = path.stat().st_size
    part = _plan(out, size, part_bytes)
    tasks = [
        Task(source, path, i, i * part, min((i + 1) * part, size), out)
        for i in range(max(1, math.ceil(size / part)))
        if not (out / _part_name(i)).is_file()
    ]
    if workers == 1:
        for task in tasks:
            _write_part(task)
    else:
        _run_ray(tasks, workers)
    (out / DONE).write_text("", encoding="utf-8")
    if path != snap.path:
        path.unlink()
    return out


def done_dir(root: Path, source: str) -> tuple[Path, str]:
    """Return a finished extract of the newest snapshot of ``source`` and the snapshot's date.

    Raises
    ------
    StoreBuildError
        If the snapshot is missing or not extracted yet; the message names the command.
    """
    snap = snapshots.snapshot(root, source)
    out = extract_dir(root, source, snap.date)
    if not (out / DONE).is_file():
        raise StoreBuildError(
            f"the {source} snapshot of {snap.date} is not extracted; "
            f"run `labdata store extract --source {source}` first"
        )
    return out, snap.date


def parts(folder: Path) -> str:
    """Return the SQL string literal of a folder's part glob."""
    return sql_string(str(folder / "part-*.parquet"))


def sql_string(text: str) -> str:
    """Return ``text`` as a quoted SQL string literal."""
    return "'" + text.replace("'", "''") + "'"


def connect(root: Path) -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB with a memory cap that spills to ``<root>/work/tmp``."""
    temp = root / "work" / "tmp"
    temp.mkdir(parents=True, exist_ok=True)
    memory = os.environ.get("LABDATA_DUCKDB_MEMORY") or DUCKDB_MEMORY
    return duckdb.connect(":memory:", config={"memory_limit": memory, "temp_directory": str(temp)})


def lookup(root: Path, source: str, wanted: Iterable[str]) -> tuple[str, list[tuple[object, ...]]]:
    """Return a snapshot's date and its extract rows for the wanted accessions.

    Raises
    ------
    StoreBuildError
        If the source is not extracted.
    """
    folder, date = done_dir(root, source)
    want = sorted(set(wanted))
    if not want:
        return date, []
    listed = ", ".join(sql_string(a) for a in want)
    con = connect(root)
    try:
        rows = con.execute(
            f"SELECT * FROM read_parquet({parts(folder)}) WHERE accession IN ({listed})"
        ).fetchall()
    finally:
        con.close()
    return date, rows


def _part_name(index: int) -> str:
    return f"part-{index:05d}.parquet"


def _plan(out: Path, size: int, part_bytes: int) -> int:
    """Save or read the range plan; return its part size."""
    plan = out / _PLAN
    if plan.is_file():
        saved = json.loads(plan.read_text(encoding="utf-8"))
        if saved["file_bytes"] != size:
            raise StoreBuildError(
                f"{out} was planned for a {saved['file_bytes']}-byte file, not {size}; "
                "delete the folder to extract again"
            )
        return int(saved["part_bytes"])
    tmp = plan.with_name(f".{_PLAN}.tmp")
    tmp.write_text(json.dumps({"file_bytes": size, "part_bytes": part_bytes}), encoding="utf-8")
    tmp.replace(plan)
    return part_bytes


def _plain(root: Path, snap: snapshots.Snapshot, workers: int) -> Path:
    """Return the snapshot as a plain file, decompressing a ``.gz`` into ``work/tmp`` once."""
    if snap.path.suffix != ".gz":
        return snap.path
    out = root / "work" / "tmp" / f"{snap.path.parent.parent.name}-{snap.date}-{snap.path.stem}"
    if out.is_file():
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f".{out.name}.tmp")
    if shutil.which("rapidgzip"):
        command = ["rapidgzip", "-d", "-c", "-P", str(workers), str(snap.path)]
    else:
        command = ["pigz", "-dc", str(snap.path)]
    try:
        with tmp.open("wb") as fh:
            subprocess.run(command, stdout=fh, check=True)
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    return out


def _run_ray(tasks: list[Task], workers: int) -> None:
    import ray

    started = not ray.is_initialized()
    if started:
        ray.init(
            num_cpus=None if os.environ.get("RAY_ADDRESS") else workers,
            include_dashboard=False,
            log_to_driver=False,
        )
    try:
        remote = ray.remote(num_cpus=1)(_write_part)
        ray.get([remote.remote(task) for task in tasks])
    finally:
        if started:
            ray.shutdown()


def _write_part(task: Task) -> None:
    """Write one part by a temporary file renamed into place."""
    schema, rows = _READERS[task.source]
    columns: dict[str, list[str | None]] = {name: [] for name in schema.names}
    with task.path.open("rb") as fh:
        for row in rows(fh, task.start, task.end):
            for name, value in zip(schema.names, row, strict=True):
                columns[name].append(value)
    final = task.out / _part_name(task.index)
    tmp = final.with_name(f".{final.name}.tmp")
    pq.write_table(pa.table(columns, schema=schema), tmp)
    tmp.replace(final)


def _lines(fh: BinaryIO, start: int, end: int) -> Iterator[bytes]:
    """Yield the lines that start in ``[start, end)``."""
    if start:
        fh.seek(start - 1)
        fh.readline()
    pos = fh.tell()
    while pos < end:
        line = fh.readline()
        if not line:
            return
        pos += len(line)
        yield line


def _elements(fh: BinaryIO, start: int, end: int, tag: bytes) -> Iterator[bytes]:
    """Yield each ``<tag …>…</tag>`` element that starts in ``[start, end)``, exactly as stored.

    Elements of this name must not nest.
    """
    head = re.compile(b"<" + tag + rb"[\s>]")
    close = b"</" + tag + b">"
    keep = len(tag) + 1
    fh.seek(start)
    buf, base, pos, eof = b"", start, 0, False
    while True:
        found = head.search(buf, pos)
        if found is not None:
            if base + found.start() >= end:
                return
            stop = buf.find(close, found.end())
            if stop >= 0:
                pos = stop + len(close)
                yield buf[found.start() : pos]
                continue
            if eof:
                raise StoreBuildError(f"unclosed <{tag.decode()}> at byte {base + found.start()}")
            cut = found.start()
        else:
            if eof:
                return
            cut = max(pos, len(buf) - keep)
            if base + cut >= end:
                return
        buf, base, pos = buf[cut:], base + cut, 0
        chunk = fh.read(_CHUNK)
        eof = not chunk
        buf += chunk


def _xml_rows(
    tag: bytes, key: re.Pattern[bytes]
) -> Callable[[BinaryIO, int, int], Iterator[tuple[str | None, ...]]]:
    def rows(fh: BinaryIO, start: int, end: int) -> Iterator[tuple[str | None, ...]]:
        for element in _elements(fh, start, end, tag):
            match = key.search(element)
            if match:
                yield match[1].decode("ascii"), element.decode("utf-8")

    return rows


def _tab_rows(fh: BinaryIO, start: int, end: int) -> Iterator[tuple[str | None, ...]]:
    """Yield each data line's accession and typed columns; a ``-`` cell reads as empty."""
    fh.seek(0)
    header = fh.readline()
    names = header.decode("utf-8").rstrip("\n").split("\t")
    at = [names.index(n) for n in ("Status", "Received", "Updated", "Published")]
    for line in _lines(fh, max(start, len(header)), end):
        cells = line.decode("utf-8").rstrip("\n").split("\t")
        accession = cells[0].strip()
        if not accession:
            continue
        values = [cells[i] if i < len(cells) and cells[i] != "-" else "" for i in at]
        status, received, updated, published = values
        yield (
            accession,
            status or None,
            received[:10] or None,
            updated[:10] or None,
            published[:10] or None,
        )


def _geo_rows(fh: BinaryIO, start: int, end: int) -> Iterator[tuple[str | None, ...]]:
    for line in _lines(fh, start, end):
        if not line.strip():
            continue
        record = json.loads(line)
        yield (record["accession"],)
        for sample in record.get("samples") or ():
            yield (sample["accession"],)
        for gpl in (record.get("gpl") or "").split(";"):
            if gpl.strip():
                yield (f"GPL{gpl.strip()}",)


_READERS: dict[
    str, tuple[pa.Schema, Callable[[BinaryIO, int, int], Iterator[tuple[str | None, ...]]]]
] = {
    snapshots.BIOSAMPLE: (_TEXT_SCHEMA, _xml_rows(b"BioSample", _BIOSAMPLE_KEY)),
    snapshots.BIOPROJECT: (_TEXT_SCHEMA, _xml_rows(b"Package", _BIOPROJECT_KEY)),
    snapshots.SRA_ACCESSIONS: (_TAB_SCHEMA, _tab_rows),
    snapshots.GEO_SERIES: (_GEO_SCHEMA, _geo_rows),
}
