"""Extract each bulk snapshot once, in parallel, into Parquet parts the build reads.

One pass per snapshot writes ``<root>/work/extract/<source>-<date>/part-NNNNN.parquet``, one
part per byte range of the (decompressed) snapshot: a part holds each record that starts in
its range, so each record lands in exactly one part. Records are found by byte scanning.

A plain snapshot is read by range, a worker seeking to each. A gzip snapshot is decompressed
as one stream and cut into the same ranges on the fly; no unpacked copy is written. Every step
resumes: each part appears by a rename and is skipped when present, and ``_DONE`` marks the
source finished. The part size is saved before the first part, so a rerun cuts the same way.
"""

from __future__ import annotations

import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, cast

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from labdata.exceptions import StoreBuildError
from labdata.store import snapshots
from labdata.store._files import temp_beside, write_atomic

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

#: Rows per row group of a part, so a reader holds a few thousand records, not a whole part.
ROW_GROUP_ROWS = 4096

#: The memory (MB) and threads a pipeline step was given; set by the pipeline for each step.
MEMORY_ENV = "LABDATA_MEMORY_MB"
THREADS_ENV = "LABDATA_THREADS"

#: The share of a step's memory DuckDB may use.
MEMORY_SHARE = 0.5
#: The share of a step's memory Ray's object store may use. The rest is for Python.
OBJECT_STORE_SHARE = 0.2

_PLAN = "_plan.json"
_RAY_MIN_STORE = 100 << 20
_RAY_MAC_STORE = 2 << 30
_CHUNK = 1 << 24
_BIOPROJECT_KEY = re.compile(rb'<ArchiveID\b[^>]*?\baccession="(PRJ[A-Z]+\d+)"')
_BIOSAMPLE_KEY = re.compile(rb'<BioSample\b[^>]*?\baccession="(SAM[A-Z]+\d+)"')
_TAGS = {
    snapshots.BIOSAMPLE: re.compile(rb"<BioSample[\s>]"),
    snapshots.BIOPROJECT: re.compile(rb"<Package[\s>]"),
}

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
    if snap.path.suffix == ".gz":
        part = _plan(out, None, part_bytes)
        with _decompressed(snap.path, workers) as stream:
            calls = (
                (source, block, out / _part_name(i))
                for i, block in _blocks(stream, source, part)
                if not (out / _part_name(i)).is_file()
            )
            run_parallel(_write_block, calls, workers)
    else:
        size = snap.path.stat().st_size
        part = _plan(out, size, part_bytes)
        tasks = (
            (Task(source, snap.path, i, i * part, min((i + 1) * part, size), out),)
            for i in range(max(1, math.ceil(size / part)))
            if not (out / _part_name(i)).is_file()
        )
        run_parallel(_write_part, tasks, workers)
    write_atomic(out / DONE, "")
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


def step_memory_mb() -> int:
    """Return this step's memory in MB: ``LABDATA_MEMORY_MB``, else :func:`allocation_mb`."""
    given = os.environ.get(MEMORY_ENV, "")
    return int(given) if given.isdigit() and int(given) > 0 else allocation_mb()


def step_threads() -> int:
    """Return this step's threads: ``LABDATA_THREADS``, else :func:`default_workers`."""
    given = os.environ.get(THREADS_ENV, "")
    return int(given) if given.isdigit() and int(given) > 0 else default_workers()


def duckdb_memory() -> str:
    """Return DuckDB's memory cap: ``LABDATA_DUCKDB_MEMORY``, else a share of the step's memory."""
    override = os.environ.get("LABDATA_DUCKDB_MEMORY")
    if override:
        return override
    return f"{max(int(step_memory_mb() * MEMORY_SHARE), 256)}MB"


def allocation_mb() -> int:
    """Return the job's memory in MB: ``SLURM_MEM_PER_NODE`` in a Slurm job.

    Outside one it is the cgroup limit or the machine's memory, whichever is smaller.
    """
    slurm = os.environ.get("SLURM_MEM_PER_NODE", "")
    if slurm.isdigit() and int(slurm) > 0:
        return int(slurm)
    limits = [os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")]
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            text = Path(path).read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if text.isdigit():
            limits.append(int(text))
    return min(limits) >> 20


@contextmanager
def connect(root: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """Yield an in-memory DuckDB with a memory cap that spills to ``<root>/work/tmp/<pid>``.

    It runs on :func:`step_threads` threads. Each process spills to its own folder, removed
    on exit. Insertion order is not kept, so a query that needs an order says ``ORDER BY``.
    """
    temp = root / "work" / "tmp" / str(os.getpid())
    temp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(
        ":memory:",
        config={
            "memory_limit": duckdb_memory(),
            "temp_directory": str(temp),
            "preserve_insertion_order": False,
            "threads": step_threads(),
        },
    )
    try:
        yield con
    finally:
        con.close()
        shutil.rmtree(temp, ignore_errors=True)


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
    with connect(root) as con:
        rows = con.execute(
            f"SELECT * FROM read_parquet({parts(folder)}) WHERE accession IN ({listed})"
        ).fetchall()
    return date, rows


def _part_name(index: int) -> str:
    return f"part-{index:05d}.parquet"


def _plan(out: Path, size: int | None, part_bytes: int) -> int:
    """Save or read the range plan; return its part size. A streamed file has no size."""
    plan = out / _PLAN
    if plan.is_file():
        saved = json.loads(plan.read_text(encoding="utf-8"))
        if saved.get("file_bytes") != size:
            raise StoreBuildError(
                f"{out} was planned for a {saved['file_bytes']}-byte file, not {size}; "
                "delete the folder to extract again"
            )
        return int(saved["part_bytes"])
    write_atomic(plan, json.dumps({"file_bytes": size, "part_bytes": part_bytes}))
    return part_bytes


@contextmanager
def _decompressed(path: Path, workers: int) -> Iterator[BinaryIO]:
    """Yield a gzip file's decompressed bytes as a stream from rapidgzip, else pigz."""
    if shutil.which("rapidgzip"):
        command = ["rapidgzip", "-d", "-c", "-P", str(workers), str(path)]
    else:
        command = ["pigz", "-dc", str(path)]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
    assert proc.stdout is not None
    try:
        yield cast(BinaryIO, proc.stdout)
    except BaseException:
        proc.kill()
        raise
    finally:
        proc.stdout.close()
        code = proc.wait()
    if code:
        raise StoreBuildError(f"{command[0]} failed on {path} (exit {code})")


def _next_start(source: str, buf: bytearray, pos: int) -> int | None:
    """Return the first record start at or after ``pos`` in ``buf``, or None if not yet seen."""
    if source in _TAGS:
        found = _TAGS[source].search(buf, pos)
        return None if found is None else found.start()
    if pos == 0:
        return 0
    newline = buf.find(b"\n", pos - 1)
    return None if newline < 0 else newline + 1


def _blocks(stream: BinaryIO, source: str, part: int) -> Iterator[tuple[int, bytes]]:
    """Cut a stream into each part's bytes.

    Part ``i`` runs from the first record start at or after ``i * part`` to the first at or
    after ``(i + 1) * part``, so it holds the records a range read of ``[i * part,
    (i + 1) * part)`` would.
    """
    if source not in _TAGS and source != snapshots.GEO_SERIES:
        raise StoreBuildError(f"the {source} snapshot cannot be extracted from a gzip stream")
    buf, base, index, eof = bytearray(), 0, 0, False
    while True:
        if eof and not buf:
            for i in range(index, max(1, -(-base // part))):
                yield i, b""
            return
        target = (index + 1) * part
        cut = _next_start(source, buf, target - base) if base + len(buf) >= target else None
        if cut is None:
            if not eof:
                chunk = stream.read(_CHUNK)
                buf += chunk
                eof = not chunk
                continue
            cut = len(buf)
        yield index, bytes(buf[:cut])
        del buf[:cut]
        base += cut
        index += 1


@contextmanager
def parallel(workers: int) -> Iterator[None]:
    """Hold one Ray session for ``workers`` across several :func:`run_parallel` calls.

    A session this starts caps its object store at a share of the step's memory.
    """
    if workers == 1:
        yield
        return
    import ray

    started = not ray.is_initialized()
    if started:
        local: dict[str, Any] = {}
        if not os.environ.get("RAY_ADDRESS"):
            store = max(int(step_memory_mb() * OBJECT_STORE_SHARE) << 20, _RAY_MIN_STORE)
            if sys.platform == "darwin":  # Ray refuses a larger store on macOS
                store = min(store, _RAY_MAC_STORE)
            local = {"num_cpus": workers, "object_store_memory": store}
        ray.init(include_dashboard=False, log_to_driver=False, **local)
    try:
        yield
    finally:
        if started:
            ray.shutdown()


def run_parallel[T](
    fn: Callable[..., T], calls: Iterable[tuple[object, ...]], workers: int
) -> list[T]:
    """Return ``fn(*args)`` for each call, in order, on up to ``workers`` Ray workers.

    ``calls`` is consumed lazily and at most ``workers`` calls are in flight, so a generator of
    large arguments stays bounded. ``workers == 1`` runs in this process without Ray.
    """
    if workers == 1:
        return [fn(*args) for args in calls]
    import ray

    with parallel(workers):
        remote = ray.remote(num_cpus=1)(fn)
        refs: list[Any] = []
        pending: list[Any] = []
        for args in calls:
            if len(pending) >= workers:
                _, pending = ray.wait(pending, num_returns=1)
            refs.append(remote.remote(*args))
            pending.append(refs[-1])
        return ray.get(refs)


def _write_part(task: Task) -> None:
    """Write one part of a plain snapshot file."""
    with task.path.open("rb") as fh:
        _write_rows(task.source, fh, task.start, task.end, task.out / _part_name(task.index))


def _write_block(source: str, block: bytes, final: Path) -> None:
    """Write one part from its bytes cut out of a decompressed stream."""
    _write_rows(source, io.BytesIO(block), 0, len(block), final)


def _write_rows(source: str, fh: BinaryIO, start: int, end: int, final: Path) -> None:
    """Write the rows of the records starting in ``[start, end)`` by a renamed temporary file."""
    schema, rows = _READERS[source]
    columns: dict[str, list[str | None]] = {name: [] for name in schema.names}
    for row in rows(fh, start, end):
        for name, value in zip(schema.names, row, strict=True):
            columns[name].append(value)
    tmp = temp_beside(final)
    try:
        pq.write_table(pa.table(columns, schema=schema), tmp, row_group_size=ROW_GROUP_ROWS)
        tmp.replace(final)
    finally:
        tmp.unlink(missing_ok=True)


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
