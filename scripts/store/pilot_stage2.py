"""Measure the stage 2 fetch and the stage 1 build on seeded samples of GEO sequencing Series.

The pilot runs in its own store root and never writes the main store. ``setup`` makes the
pilot root and links the main store's ``snapshots/`` and ``work/extract/`` into it. ``draw``
takes a seeded, nested fraction of the sequencing Series for one step.

``concurrent`` fills the pilot's ``raw/`` the stage 2 way. Family SOFT and ``filelist.txt``
come over a few kept-open HTTPS connections, with the SOFT data tables stripped as it
streams. The E-utilities calls, a combined ``elink`` per Series and the SRA package
``efetch``, are POSTs paced by one shared rate (:class:`EUtils`); PubMed ``[si]`` searches
wait for stage 4. The workers return results, and
the script alone writes ``raw/``, one :func:`labdata.store.raw.write_rows` call per source
(the pack). ``elink-extra`` counts the links ``elink`` adds beyond the SOFT relation lines
and ``SRA_Accessions.tab``.

``shard`` times the serial stage 1 fetch, :func:`labdata.store.fetch.fetch`, on one shard of
a step, in the shard's own root. ``build`` times one build and its peak memory. ``report``
writes the step's ``results.json`` and ``results.md``. A step's files go under
``<pilot>/pilot/<step>/``.

Usage, each under ``pixi run python scripts/store/``::

    pilot_stage2.py setup --main MAIN --pilot P
    pilot_stage2.py draw --pilot P --name step1 --fraction 0.001
    pilot_stage2.py concurrent --pilot P --name step1 --connections 2 --inflight 2
    pilot_stage2.py shard --pilot P --name step1 --index 0 --count 4
    pilot_stage2.py build --pilot P --build-id pilot-1
    pilot_stage2.py report --pilot P --name step1
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import gzip
import hashlib
import http.client
import json
import logging
import math
import os
import queue
import random
import re
import resource
import socket
import sys
import threading
import time
import urllib.parse
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from labdata.geo.sra_records import split_packages
from labdata.ncbi import entrez
from labdata.ncbi.config import load_credentials
from labdata.ncbi.entrez import EntrezClient
from labdata.store import build, extract, fetch, init_store, provenance, raw, snapshots, sources
from labdata.store._files import write_atomic
from labdata.store.root import MARKER

SEQUENCING = "high throughput sequencing"
DEFAULT_SEED = 50
DEFAULT_CHUNK = 50
#: The GEO-linked SRA experiment count the SRA projection scales to; pass the current count.
DEFAULT_EXPERIMENTS = 5_640_000
GEO_SITE = "https://ftp.ncbi.nlm.nih.gov"
_TABLE_BEGIN = (b"!sample_table_begin", b"!platform_table_begin")
_TABLE_END = (b"!sample_table_end", b"!platform_table_end")
_THROTTLED = (429, 503)
_USER_AGENT = "liulab-data (+https://github.com/liuhlab/liulab-data)"
_RAW_SOURCES = (raw.GEO_SOFT, raw.GEO_FILELIST, raw.ENTREZ_LINKS, raw.SRA_XML, raw.PUBMED_SI)
#: The sources the concurrent fetch fills; PubMed ``[si]`` waits for stage 4.
_FETCHED = (raw.GEO_SOFT, raw.GEO_FILELIST, raw.ENTREZ_LINKS, raw.SRA_XML)
_SOFT_SRX = re.compile(r"^!Sample_relation = SRA: \S*?[?&]term=([SED]RX\d+)", re.M)


# ---------- targets ----------


@dataclass(frozen=True, slots=True)
class Target:
    """One target Series and its ``esummary`` type."""

    accession: str
    gdstype: str

    @property
    def mixed(self) -> bool:
        """Whether the Series also has an array type."""
        return "array" in self.gdstype.lower()


def sequencing_series(lines: Iterable[str]) -> list[Target]:
    """Return the Series whose ``esummary`` type names high-throughput sequencing, by number."""
    out = []
    for line in lines:
        if not line.strip():
            continue
        record = json.loads(line)
        kind = record.get("gdstype") or ""
        if SEQUENCING in kind.lower():
            out.append(Target(record["accession"], kind))
    return sorted(out, key=lambda t: int(t.accession[3:]))


def draw(pool: list[Target], fraction: float, seed: int, size: int | None = None) -> list[Target]:
    """Return a seeded random ``fraction`` of ``pool``, or ``size`` Series, sorted by number.

    The draw is a prefix of one seeded shuffle, so a smaller draw lies inside a larger one.
    """
    k = size if size is not None else max(1, round(len(pool) * fraction))
    order = list(pool)
    random.Random(seed).shuffle(order)
    return sorted(order[:k], key=lambda t: int(t.accession[3:]))


def chunks(items: list[str], size: int) -> list[list[str]]:
    """Split ``items`` into consecutive chunks of at most ``size``."""
    return [items[i : i + size] for i in range(0, len(items), size)]


# ---------- metering ----------


@dataclass(slots=True)
class StepStats:
    """Calls, seconds, bytes returned and errors by exception class for one fetch step."""

    calls: int = 0
    seconds: float = 0.0
    bytes: int = 0
    errors: Counter[str] = field(default_factory=Counter)


def step_of(method: str, url: str = "") -> str:
    """Return the raw source a client call feeds: the step it is timed under."""
    if method == "fetch_text":
        return raw.GEO_FILELIST if url.endswith("filelist.txt") else raw.GEO_SOFT
    return {
        "efetch": raw.SRA_XML,
        "esearch": raw.PUBMED_SI,
        "elink_many": raw.ENTREZ_LINKS,
        "esummary_many": raw.ENTREZ_LINKS,
    }.get(method, method)


class MeteredClient(EntrezClient):
    """An :class:`EntrezClient` that times each call by step and counts failures."""

    def __init__(self, inner: EntrezClient) -> None:
        # Delegates every call; holds no credentials or cache of its own.
        self.inner = inner
        self.stats: dict[str, StepStats] = defaultdict(StepStats)
        self.last_failed: str | None = None
        self._lock = threading.Lock()

    def _call(self, step: str, fn: Any, *args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        try:
            out = fn(*args, **kwargs)
        except Exception as exc:
            with self._lock:
                stats = self.stats[step]
                stats.calls += 1
                stats.seconds += time.perf_counter() - start
                stats.errors[type(exc).__name__] += 1
                self.last_failed = step
            raise
        with self._lock:
            stats = self.stats[step]
            stats.calls += 1
            stats.seconds += time.perf_counter() - start
            stats.bytes += len(out.encode("utf-8")) if isinstance(out, str) else 0
        return out

    def clear_cache(self) -> None:
        """Drop the inner client's cached responses."""
        self.inner.clear_cache()

    def esearch(self, db: str, term: str, *, retmax: int = 20) -> list[str]:
        """Time :meth:`EntrezClient.esearch`."""
        return self._call(step_of("esearch"), self.inner.esearch, db, term, retmax=retmax)

    def esummary_many(self, db: str, uids: list[str]) -> list[dict[str, Any]]:
        """Time :meth:`EntrezClient.esummary_many`."""
        return self._call(step_of("esummary_many"), self.inner.esummary_many, db, uids)

    def elink_many(self, dbfrom: str, db: str, uids: list[str]) -> dict[str, list[str]]:
        """Time :meth:`EntrezClient.elink_many`."""
        return self._call(step_of("elink_many"), self.inner.elink_many, dbfrom, db, uids)

    def efetch(
        self, db: str, ids: list[str], *, rettype: str, retmode: str = "text", refresh: bool = False
    ) -> str:
        """Time :meth:`EntrezClient.efetch`."""
        return self._call(
            step_of("efetch"),
            self.inner.efetch,
            db,
            ids,
            rettype=rettype,
            retmode=retmode,
            refresh=refresh,
        )

    def fetch_text(self, url: str) -> str:
        """Time :meth:`EntrezClient.fetch_text`."""
        return self._call(step_of("fetch_text", url), self.inner.fetch_text, url)


#: Every transient failure the Entrez seam retried, by kind; see :func:`_record_transient`.
TRANSIENT: Counter[str] = Counter()


def _record_transient(err: BaseException) -> bool:
    """Count a failure the client is about to judge, then judge it as the client would."""
    verdict = _is_transient(err)
    if verdict:
        code = getattr(err, "code", None)
        TRANSIENT[f"HTTP {code}" if code else type(err).__name__] += 1
    return verdict


_is_transient = entrez._is_transient
# Measuring only: the client still decides; this sees the 429s and drops it retries past.
entrez._is_transient = _record_transient

#: Seconds a socket may wait. Bio.Entrez opens URLs with no timeout, so a connection whose
#: packets are lost hangs a worker until TCP gives up; a timeout lets the client retry it.
SOCKET_TIMEOUT = 30.0
socket.setdefaulttimeout(SOCKET_TIMEOUT)


class _SkipLog(logging.Handler):
    """Collect the Series the stage 1 fetch skips because their SOFT type is not sequencing."""

    def __init__(self) -> None:
        super().__init__(logging.INFO)
        self.skipped: dict[str, str] = {}

    def emit(self, record: logging.LogRecord) -> None:
        if str(record.msg).startswith("skipped") and isinstance(record.args, tuple):
            self.skipped[str(record.args[0])] = str(record.args[1])


def peak_rss() -> int:
    """Return this process's peak resident memory in bytes."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024


# ---------- setup and targets ----------


def pilot_dir(pilot: Path, step: str | None = None) -> Path:
    """Return the folder of the pilot's own files, or of one step's targets, logs and results."""
    return pilot / "pilot" if step is None else pilot / "pilot" / step


def _link_inputs(root: Path, main: Path) -> None:
    (root / "work").mkdir(exist_ok=True)
    for name in ("snapshots", "work/extract"):
        link, target = root / name, (main / name).resolve()
        if not link.is_symlink():
            link.symlink_to(target, target_is_directory=True)
        elif link.resolve() != target:
            raise SystemExit(f"{link} points at {link.resolve()}, not {target}")


def setup(main: Path, pilot: Path) -> None:
    """Make the pilot store and link the main store's snapshots and extracts into it."""
    if main.resolve() == pilot.resolve():
        raise SystemExit("the pilot root must differ from the main store root")
    if not (main / MARKER).is_file():
        raise SystemExit(f"{main} is not a labdata store: it has no {MARKER}")
    if not (pilot / MARKER).is_file():
        init_store(pilot)
    _link_inputs(pilot, main)
    pilot_dir(pilot).mkdir(exist_ok=True)


def draw_step(
    pilot: Path,
    step: str,
    fraction: float,
    seed: int = DEFAULT_SEED,
    chunk: int = DEFAULT_CHUNK,
    size: int | None = None,
) -> dict[str, Any]:
    """Draw one step's targets into its folder; a rerun with other settings is refused.

    ``size``, when given, takes that many Series instead of ``fraction`` of them.
    """
    out = pilot_dir(pilot, step)
    out.mkdir(parents=True, exist_ok=True)
    meta_path = out / "targets.json"
    if meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        asked = {"fraction": fraction, "seed": seed, "chunk": chunk, "size": size}
        if any(meta[k] != v for k, v in asked.items()):
            raise SystemExit(f"{meta_path} was drawn with other settings: {meta}")
        return meta
    snap = snapshots.snapshot(pilot, snapshots.GEO_SERIES)
    with gzip.open(snap.path, "rt", encoding="utf-8") as fh:
        pool = sequencing_series(fh)
    chosen = draw(pool, fraction, seed, size)
    write_atomic(
        out / "targets.tsv",
        "accession\tmixed\tgdstype\n"
        + "".join(f"{t.accession}\t{int(t.mixed)}\t{t.gdstype}\n" for t in chosen),
    )
    meta = {
        "snapshot": str(snap.path.relative_to(pilot)),
        "snapshot_date": snap.date,
        "sequencing_series": len(pool),
        "sequencing_mixed": sum(t.mixed for t in pool),
        "fraction": fraction,
        "size": size,
        "seed": seed,
        "chunk": chunk,
        "drawn": len(chosen),
        "drawn_mixed": sum(t.mixed for t in chosen),
    }
    write_atomic(meta_path, json.dumps(meta, indent=2) + "\n")
    return meta


def read_targets(pilot: Path, step: str) -> dict[str, bool]:
    """Return each target Series of a step and whether it is mixed sequencing and array."""
    lines = (pilot_dir(pilot, step) / "targets.tsv").read_text(encoding="utf-8").splitlines()[1:]
    return {acc: mixed == "1" for acc, mixed, _ in (line.split("\t", 2) for line in lines)}


# ---------- the serial stage 1 fetch, sharded ----------


def read_log(path: Path) -> list[dict[str, Any]]:
    """Return the chunk entries of a fetch log; a torn last line is ignored."""
    if not path.is_file():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def run_fetch(root: Path, step: str, client: EntrezClient) -> Path:
    """Run the stage 1 fetch on every target chunk not yet logged, one log line per chunk."""
    meta = json.loads((pilot_dir(root, step) / "targets.json").read_text(encoding="utf-8"))
    log_path = pilot_dir(root, step) / "fetch-log.jsonl"
    done = {entry["chunk"] for entry in read_log(log_path)}
    skips = _SkipLog()
    logger = logging.getLogger(fetch.__name__)
    logger.addHandler(skips)
    logger.setLevel(logging.INFO)
    for index, chunk in enumerate(chunks(sorted(read_targets(root, step)), meta["chunk"])):
        if index in done:
            continue
        metered = MeteredClient(client)
        retried = TRANSIENT.copy()
        skips.skipped = {}
        failed: dict[str, dict[str, str]] = {}
        start = time.perf_counter()
        try:
            fetch.fetch(root, chunk, client=metered)
        except Exception:
            # Fetch the chunk Series by Series so each failure is pinned to one Series.
            for gse in chunk:
                metered.last_failed = None
                try:
                    fetch.fetch(root, [gse], client=metered)
                except Exception as exc:
                    failed[gse] = {
                        "step": metered.last_failed or "parse",
                        "error": type(exc).__name__,
                        "message": str(exc)[:300],
                    }
        entry = {
            "chunk": index,
            "series": chunk,
            "wall_s": round(time.perf_counter() - start, 3),
            "failed": failed,
            "soft_type_skipped": dict(skips.skipped),
            "steps": {
                name: {
                    "calls": s.calls,
                    "seconds": round(s.seconds, 3),
                    "bytes": s.bytes,
                    "errors": dict(s.errors),
                }
                for name, s in sorted(metered.stats.items())
            },
            "retried": dict(TRANSIENT - retried),
            "maxrss_bytes": peak_rss(),
            "job": os.environ.get("SLURM_JOB_ID"),
            "restart": os.environ.get("SLURM_RESTART_COUNT", "0"),
        }
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        client.clear_cache()
        print(f"chunk {index}: {len(chunk)} Series, {entry['wall_s']} s, {len(failed)} failed")
    return log_path


def shard_root(pilot: Path, step: str, index: int) -> Path:
    """Return one shard's own store root: ``<pilot>/shards/<step>/<index>/``."""
    return pilot / "shards" / step / str(index)


def run_shard(pilot: Path, step: str, index: int, count: int, client: EntrezClient) -> Path:
    """Run the stage 1 fetch on shard ``index`` of ``count`` in the shard's own root.

    The shard takes every ``count``-th target from ``index`` on and never writes the pilot
    root. Returns the shard's fetch log.
    """
    if not 0 <= index < count:
        raise SystemExit(f"shard {index} is outside 0..{count - 1}")
    root = shard_root(pilot, step, index)
    if not (root / MARKER).is_file():
        init_store(root)
    _link_inputs(root, pilot)
    out = pilot_dir(root, step)
    out.mkdir(parents=True, exist_ok=True)
    meta = json.loads((pilot_dir(pilot, step) / "targets.json").read_text(encoding="utf-8"))
    lines = (pilot_dir(pilot, step) / "targets.tsv").read_text(encoding="utf-8").splitlines()
    header, rows = lines[0], sorted(lines[1:])
    write_atomic(out / "targets.tsv", "".join(f"{x}\n" for x in [header, *rows[index::count]]))
    write_atomic(out / "targets.json", json.dumps(meta | {"shard": [index, count]}) + "\n")
    return run_fetch(root, step, client)


def fetch_logs(pilot: Path, step: str) -> dict[str, list[dict[str, Any]]]:
    """Return the stage 1 fetch log entries of a step's shards, keyed by shard index."""
    logs = {
        folder.name: read_log(pilot_dir(folder, step) / "fetch-log.jsonl")
        for folder in sorted((pilot / "shards" / step).glob("*"))
    }
    return {k: v for k, v in logs.items() if v}


# ---------- the concurrent fetch ----------


class _ThrottledError(Exception):
    """The server answered 429 or 503."""


@dataclass(frozen=True, slots=True)
class Soft:
    """One family SOFT: its text without data tables, and the original's size and sha256."""

    text: str
    original_bytes: int
    original_sha256: str
    gz_bytes: int


def read_soft(resp: Any) -> tuple[Soft, int]:
    """Stream a gzipped family SOFT, dropping sample and platform data table blocks.

    The begin and end marker lines go with their block; every other line is kept.
    """
    gz_bytes = int(resp.getheader("Content-Length") or 0)
    digest = hashlib.sha256()
    size = 0
    kept: list[bytes] = []
    inside = False
    with gzip.GzipFile(fileobj=resp) as fh:
        for line in fh:
            digest.update(line)
            size += len(line)
            head = line[:24].lower()
            if head.startswith(_TABLE_BEGIN):
                inside = True
            if not inside:
                kept.append(line)
            elif head.startswith(_TABLE_END):
                inside = False
    resp.read()
    return Soft(b"".join(kept).decode("utf-8"), size, digest.hexdigest(), gz_bytes), gz_bytes


def read_text(resp: Any) -> tuple[str, int]:
    """Read a plain body as text."""
    body = resp.read()
    return body.decode("utf-8"), len(body)


def pooled_get(
    paths: dict[str, str],
    read: Callable[[Any], tuple[Any, int]],
    *,
    connections: int,
    base: str = GEO_SITE,
    timeout: float = 60.0,
    tries: int = 3,
    phase: str = "get",
    every: int = 100,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """GET each path over ``connections`` kept-open connections and return what ``read`` made.

    A 404 gives ``None``. A 429, 503 or timeout halves the connections in use, and the item
    is tried again later, up to ``tries`` times. Returns the values by key and the stats.
    """
    url = urllib.parse.urlsplit(base)
    todo: queue.Queue[str] = queue.Queue()
    for key in sorted(paths):
        todo.put(key)
    lock = threading.Lock()
    limit = [connections]
    attempts: Counter[str] = Counter()
    events: Counter[str] = Counter()
    values: dict[str, Any] = {}
    wire: list[int] = []
    millis: list[int] = []
    errors: dict[str, str] = {}

    def connect() -> http.client.HTTPConnection:
        if url.scheme == "https":
            return http.client.HTTPSConnection(url.netloc, timeout=timeout)
        return http.client.HTTPConnection(url.netloc, timeout=timeout)

    def get(conn: http.client.HTTPConnection, key: str) -> tuple[Any, int]:
        conn.request("GET", f"{url.path}{paths[key]}", headers={"User-Agent": _USER_AGENT})
        resp = conn.getresponse()
        if resp.status != 200:
            resp.read()
            if resp.status in _THROTTLED:
                raise _ThrottledError(str(resp.status))
            if resp.status == 404:
                return None, 0
            raise http.client.HTTPException(f"HTTP {resp.status}")
        return read(resp)

    def retry(key: str, kind: str) -> None:
        if attempts[key] < tries:
            todo.put(key)
        else:
            errors[key] = kind

    def worker(index: int) -> None:
        conn: http.client.HTTPConnection | None = None
        while index < limit[0]:
            try:
                key = todo.get_nowait()
            except queue.Empty:
                break
            attempts[key] += 1
            start = time.perf_counter()
            try:
                conn = conn or connect()
                value, size = get(conn, key)
            except (_ThrottledError, TimeoutError) as exc:
                kind = f"HTTP {exc}" if isinstance(exc, _ThrottledError) else "timeout"
                with lock:
                    events[kind] += 1
                    limit[0] = max(1, limit[0] // 2)
                say(f"{phase}: {key} {kind}; connections now {limit[0]}")
                if conn:
                    conn.close()
                conn = None
                retry(key, kind)
                time.sleep(min(30.0, 2.0 ** attempts[key]))
                continue
            except (OSError, http.client.HTTPException, EOFError, UnicodeDecodeError) as exc:
                if conn:
                    conn.close()
                conn = None
                retry(key, f"{type(exc).__name__}: {exc}"[:200])
                continue
            with lock:
                millis.append(round((time.perf_counter() - start) * 1000))
                values[key] = value
                wire.append(size)
            progress.tick()
        if conn:
            conn.close()

    progress = _Progress(phase, len(paths), every)
    start = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(connections)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    worker(0)  # an item put back by a worker that then stopped
    wall = time.perf_counter() - start
    progress.end(wall)
    got = sum(v is not None for v in values.values())
    stats = {
        "connections": connections,
        "items": got,
        "missing_404": sorted(k for k, v in values.items() if v is None),
        "failed": errors,
        "wire_bytes": sum(wire),
        "wall_s": round(wall, 3),
        "items_per_s": round(got / wall, 2) if wall else None,
        "bytes_per_s": round(sum(wire) / wall) if wall else None,
        "item_ms": quantiles(millis),
        "throttle_events": dict(events),
        "connections_at_end": limit[0],
    }
    return values, stats


def _url_path(url: str) -> str:
    return urllib.parse.urlsplit(url).path


def say(message: str) -> None:
    """Print one progress line at once, so a stall shows in the Slurm log."""
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} {message}", flush=True)


class _Progress:
    """Count finished items from many threads and print every ``every`` of them."""

    def __init__(self, phase: str, total: int, every: int) -> None:
        self.phase, self.total, self.every = phase, total, max(1, every)
        self.done = 0
        self.lock = threading.Lock()
        say(f"{phase}: start, {total} items")

    def tick(self) -> None:
        with self.lock:
            self.done += 1
            if self.done % self.every == 0:
                say(f"{self.phase}: {self.done}/{self.total}")

    def end(self, wall: float) -> None:
        say(f"{self.phase}: end, {self.done}/{self.total} in {wall:.1f} s")


def _pool(
    work: Callable[[Any], Any], items: list[Any], inflight: int, phase: str = "pool", every: int = 1
) -> tuple[list[Any], float, dict[str, int]]:
    """Run ``work`` on each item, ``inflight`` at once; failed items count by exception class."""
    errors: Counter[str] = Counter()
    progress = _Progress(phase, len(items), every)

    def safe(item: Any) -> Any:
        try:
            return work(item)
        except Exception as exc:
            errors[type(exc).__name__] += 1
            say(f"{phase}: {type(exc).__name__}: {str(exc)[:200]}")
            return None
        finally:
            progress.tick()

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=inflight) as workers:
        out = list(workers.map(safe, items))
    progress.end(time.perf_counter() - start)
    return out, time.perf_counter() - start, dict(errors)


EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
#: Requests per second for all calls together; NCBI allows 10 per API key, 3 without one.
DEFAULT_RATE = 8.0
#: IDs per esummary POST.
SUMMARY_BATCH = 500
_RATE_FLOOR = 0.5


class EUtilsError(Exception):
    """An E-utilities request failed for good."""


def _rank(ordered: list[int], q: float) -> int | None:
    if not ordered:
        return None
    return ordered[min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))]


@dataclass(slots=True)
class EStats:
    """What one E-utility did: requests, IDs per request, latency, throttles and retries."""

    requests: int = 0
    ids: list[int] = field(default_factory=list)
    millis: list[int] = field(default_factory=list)
    bytes: int = 0
    throttles: Counter[str] = field(default_factory=Counter)
    retries: Counter[str] = field(default_factory=Counter)
    first: float | None = None
    last: float = 0.0

    def summary(self) -> dict[str, Any]:
        """Return the counts, IDs per request, achieved rate and latency quantiles."""
        span = self.last - self.first if self.first is not None else 0.0
        return {
            "requests": self.requests,
            "ids_per_request": quantiles(self.ids),
            "req_per_s": round(self.requests / span, 2) if span > 0 else None,
            "latency_ms": quantiles(self.millis) | {"p95": _rank(sorted(self.millis), 0.95)},
            "bytes": self.bytes,
            "throttles": dict(self.throttles),
            "retries": dict(self.retries),
        }


def _backoff(attempt: int) -> float:
    """Exponential backoff with jitter, capped at 60 s."""
    return min(60.0, 2.0**attempt) * random.uniform(0.5, 1.0)


class EUtils:
    """NCBI E-utilities over kept-open connections: every call a POST, one shared rate.

    One token bucket paces every request of the process at ``rate`` per second, and at most
    ``inflight`` requests are open at once. A 429 or 503 halves the rate. It, another 5xx
    or a dropped connection is retried after ``Retry-After`` or an exponential backoff.
    """

    def __init__(
        self,
        *,
        email: str,
        api_key: str | None,
        rate: float = DEFAULT_RATE,
        inflight: int = 4,
        base: str = EUTILS,
        timeout: float = SOCKET_TIMEOUT,
        tries: int = 6,
        tool: str = "liulab-data",
    ) -> None:
        self.url = urllib.parse.urlsplit(base)
        self.params = {"tool": tool, "email": email} | ({"api_key": api_key} if api_key else {})
        self.rate, self.timeout, self.tries = rate, timeout, tries
        self.stats: dict[str, EStats] = defaultdict(EStats)
        self._next = 0.0
        self._lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(inflight)
        self._local = threading.local()

    def _turn(self) -> None:
        with self._lock:
            now = time.monotonic()
            at = max(now, self._next)
            self._next = at + 1.0 / self.rate
        time.sleep(max(0.0, at - now))

    def _conn(self) -> http.client.HTTPConnection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            if self.url.scheme == "https":
                conn = http.client.HTTPSConnection(self.url.netloc, timeout=self.timeout)
            else:
                conn = http.client.HTTPConnection(self.url.netloc, timeout=self.timeout)
            self._local.conn = conn
        return conn

    def _drop(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
        self._local.conn = None

    def _note(self, util: str, kind: str, *, throttled: bool) -> None:
        with self._lock:
            stats = self.stats[util]
            (stats.throttles if throttled else stats.retries)[kind] += 1
            if throttled:
                self.rate = max(_RATE_FLOOR, self.rate / 2)
        say(f"{util}: {kind}; rate now {self.rate:g}/s")

    def post(self, util: str, params: dict[str, str], ids: int = 0) -> bytes:
        """POST ``params`` to ``<util>.fcgi`` and return the body; ``ids`` is for the stats."""
        body = urllib.parse.urlencode(params | self.params).encode()
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": _USER_AGENT,
        }
        for attempt in range(1, self.tries + 1):
            self._turn()
            wait = _backoff(attempt)
            start = time.perf_counter()
            with self._lock:
                if self.stats[util].first is None:
                    self.stats[util].first = start
            try:
                with self._slots:
                    conn = self._conn()
                    conn.request("POST", f"{self.url.path}/{util}.fcgi", body, headers)
                    resp = conn.getresponse()
                    data = resp.read()
            except (OSError, http.client.HTTPException) as exc:
                self._drop()
                kind = "timeout" if isinstance(exc, TimeoutError) else type(exc).__name__
                self._note(util, kind, throttled=False)
            else:
                end = time.perf_counter()
                limited = b"API rate limit exceeded" in data[:300]
                if resp.status == 200 and not limited:
                    with self._lock:
                        stats = self.stats[util]
                        stats.requests += 1
                        stats.ids.append(ids)
                        stats.millis.append(round((end - start) * 1000))
                        stats.bytes += len(data)
                        stats.last = end
                    return data
                if resp.status < 500 and resp.status != 429 and not limited:
                    raise EUtilsError(f"{util} HTTP {resp.status}: {data[:200]!r}")
                throttled = resp.status in _THROTTLED or limited
                self._note(util, f"HTTP {resp.status}", throttled=throttled)
                with contextlib.suppress(ValueError):
                    wait = max(wait, float(resp.getheader("Retry-After") or 0))
                if resp.will_close:
                    self._drop()
            if attempt < self.tries:
                time.sleep(wait)
        raise EUtilsError(f"{util} failed {self.tries} times")

    def elink(self, dbfrom: str, db: str, uids: list[str]) -> set[str]:
        """Return every UID that any of ``uids`` links to, from one combined ``elink``.

        The UIDs go as one comma-joined ``id``, so NCBI answers one merged set; which source
        UID gave which link is lost.
        """
        if not uids:
            return set()
        params = {"dbfrom": dbfrom, "db": db, "id": ",".join(uids), "retmode": "json"}
        reply = json.loads(self.post("elink", params, len(uids)))
        return {
            str(link)
            for linkset in reply.get("linksets", [])
            for linksetdb in linkset.get("linksetdbs", [])
            if linksetdb.get("dbto") == db
            for link in linksetdb.get("links", [])
        }

    def esummary(self, db: str, uids: list[str]) -> dict[str, dict[str, Any]]:
        """Return the document summary of each UID, by UID, from one ``esummary``."""
        if not uids:
            return {}
        params = {"db": db, "id": ",".join(uids), "retmode": "json"}
        result = json.loads(self.post("esummary", params, len(uids))).get("result", {})
        return {str(u): result[str(u)] for u in result.get("uids", []) if str(u) in result}

    def efetch(self, db: str, ids: list[str], *, rettype: str, retmode: str) -> str:
        """Return one ``efetch`` reply as text."""
        params = {"db": db, "id": ",".join(ids), "rettype": rettype, "retmode": retmode}
        return self.post("efetch", params, len(ids)).decode("utf-8")

    def report(self) -> dict[str, Any]:
        """Return each E-utility's stats and the rate in force at the end."""
        return {"rate_at_end": self.rate} | {k: v.summary() for k, v in sorted(self.stats.items())}


def accession_of(db: str, doc: dict[str, Any]) -> str:
    """Return the accession in an ``sra`` or ``bioproject`` document summary."""
    if db == "sra":
        xml = str(doc.get("expxml") or doc.get("ExpXml") or "")
        match = re.search(r'<Experiment\b[^>]*?\bacc="([^"]+)"', xml)
        return match[1] if match else ""
    return str(doc.get("project_acc") or doc.get("Project_Acc") or "")


def _throttles(utils: list[str], eutils: EUtils) -> Counter[str]:
    out: Counter[str] = Counter()
    for util in utils:
        if util in eutils.stats:
            out.update(eutils.stats[util].throttles)
            out.update(eutils.stats[util].retries)
    return out


def concurrent_fetch(
    pilot: Path,
    step: str,
    eutils: EUtils,
    *,
    connections: int,
    inflight: int,
    batch: int = 300,
    base: str = GEO_SITE,
) -> dict[str, Any]:
    """Fill ``raw/`` with a step's records the stage 2 way and time each source and the pack.

    Every target and its SubSeries is stored, whatever its SOFT type says (#51 lets the
    target list decide). The result is saved as ``concurrent.json``; a rerun returns it.
    """
    out = pilot_dir(pilot, step)
    path = out / "concurrent.json"
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    date = dt.datetime.now(dt.UTC).date().isoformat()
    result: dict[str, Any] = {
        "connections": connections,
        "inflight": inflight,
        "batch": batch,
        "rate": eutils.rate,
    }
    begin = time.perf_counter()

    # Family SOFT, then the SubSeries they name, round by round.
    soft: dict[str, Soft] = {}
    rounds: list[dict[str, Any]] = []
    todo, seen = sorted(read_targets(pilot, step)), set()
    while todo:
        seen |= set(todo)
        got, stats = pooled_get(
            {g: _url_path(fetch.soft_url(g)) for g in todo},
            read_soft,
            connections=connections,
            base=base,
            phase=f"soft round {len(rounds) + 1}",
            every=50,
        )
        rounds.append(stats)
        soft |= {g: v for g, v in got.items() if v is not None}
        todo = sorted({s for v in got.values() if v for s in sources.subseries(v.text)} - seen)
    result[raw.GEO_SOFT] = _merge_rounds(rounds)
    held = sorted(soft)
    write_atomic(
        out / "soft-original.tsv",
        "accession\tgz_bytes\toriginal_bytes\tstripped_bytes\toriginal_sha256\n"
        + "".join(
            f"{g}\t{s.gz_bytes}\t{s.original_bytes}\t{len(s.text.encode('utf-8'))}\t"
            f"{s.original_sha256}\n"
            for g, s in sorted(soft.items())
        ),
    )

    files, result[raw.GEO_FILELIST] = pooled_get(
        {g: _url_path(fetch.filelist_url(g)) for g in held},
        read_text,
        connections=connections,
        base=base,
        phase="filelist",
        every=100,
    )
    filelists = {
        g: v or "" for g, v in files.items() if g not in result[raw.GEO_FILELIST]["failed"]
    }

    # elink, only to count what it adds beyond SOFT and SRA_Accessions.tab: one combined
    # call per Series for all its samples, and one for the Series itself to BioProject.
    begun = time.perf_counter()

    def to_links(gse: str) -> tuple[str, set[str], set[str]]:
        gsms = [a for e, a, _ in sources.soft_blocks(soft[gse].text) if e == "SAMPLE"]
        to_sra = eutils.elink("gds", "sra", [fetch._gds_uid(g) for g in gsms])
        return gse, to_sra, eutils.elink("gds", "bioproject", [fetch._gds_uid(gse)])

    replies, _, errors = _pool(to_links, held, inflight, "elink", every=10)
    sra_uids = {r[0]: r[1] for r in replies if r}
    bp_uids = {r[0]: r[2] for r in replies if r}

    def names(job: tuple[str, list[str]]) -> dict[str, str]:
        docs = eutils.esummary(job[0], job[1])
        return {u: a for u, d in docs.items() if (a := accession_of(job[0], d))}

    jobs = [
        (db, c)
        for db, uids in (("sra", sra_uids), ("bioproject", bp_uids))
        for c in chunks(sorted({u for v in uids.values() for u in v}), SUMMARY_BATCH)
    ]
    replies, _, name_errors = _pool(names, jobs, inflight, "esummary link names")
    accession = {u: a for r in replies if r for u, a in r.items()}
    linked: dict[str, str] = {}
    for gse in held:
        lines = [f"{gse}\tbioproject\t{u}\t{accession.get(u, '')}" for u in bp_uids.get(gse, ())]
        lines += [f"{gse}\tsra\t{u}\t{accession.get(u, '')}" for u in sra_uids.get(gse, ())]
        linked[gse] = "\n".join(sorted(lines))
    errors = dict(Counter(errors) + Counter(name_errors))
    wall = time.perf_counter() - begun
    result[raw.ENTREZ_LINKS] = _rate(
        len(linked), wall, errors, _throttles(["elink", "esummary"], eutils)
    )
    result[raw.ENTREZ_LINKS]["sample_mapping"] = "lost: sra lines name the Series as source"

    experiments = {x for s in soft.values() for x in _SOFT_SRX.findall(s.text)}
    experiments |= {
        row.to
        for text in linked.values()
        for row in sources.entrez_links(text)
        if row.relation == "SRA"
    }

    # SRA packages: one efetch POST per batch; ids a reply leaves out are asked once more.
    def packages(ids: list[str]) -> dict[str, str]:
        reply = eutils.efetch("sra", ids, rettype="full", retmode="xml")
        got = {a: t for a, t in split_packages(reply).items() if a in set(ids)}
        missing = [i for i in ids if i not in got]
        if missing:
            again = eutils.efetch("sra", missing, rettype="full", retmode="xml")
            got |= {a: t for a, t in split_packages(again).items() if a in set(missing)}
        return got

    replies, wall, errors = _pool(
        packages, chunks(sorted(experiments), batch), inflight, "efetch sra packages"
    )
    sra = {a: t for reply in replies if reply for a, t in reply.items()}
    result[raw.SRA_XML] = _rate(len(sra), wall, errors, _throttles(["efetch"], eutils))
    result[raw.SRA_XML] |= {
        "requested": len(experiments),
        "missing": len(experiments - set(sra)),
        "bytes": sum(len(t.encode("utf-8")) for t in sra.values()),
    }
    result[raw.SRA_XML]["bytes_per_s"] = round(result[raw.SRA_XML]["bytes"] / wall) if wall else 0
    result["eutils"] = eutils.report()

    # PubMed [si] searches are left out: papers belong to stage 4.
    result["fetch_wall_s"] = round(time.perf_counter() - begin, 3)

    # The pack: the single writer of raw/, one write per source.
    texts = {
        raw.GEO_SOFT: {g: s.text for g, s in soft.items()},
        raw.GEO_FILELIST: filelists,
        raw.ENTREZ_LINKS: linked,
        raw.SRA_XML: sra,
    }
    pack: dict[str, Any] = {}
    start = time.perf_counter()
    for source, rows in texts.items():
        began = time.perf_counter()
        say(f"pack {source}: {len(rows)} rows")
        written = raw.write_rows(pilot, source, [raw.RawRow(a, t, date) for a, t in rows.items()])
        provenance.document_raw(pilot, source)
        pack[source] = {
            "rows": len(rows),
            "buckets": len(written),
            "seconds": round(time.perf_counter() - began, 3),
        }
    result["pack"] = {"wall_s": round(time.perf_counter() - start, 3), "sources": pack}
    result["wall_s"] = round(time.perf_counter() - begin, 3)
    result["maxrss_bytes"] = peak_rss()
    result["job"] = os.environ.get("SLURM_JOB_ID")
    write_atomic(path, json.dumps(result, indent=2) + "\n")
    return result


def _rate(items: int, wall: float, errors: dict[str, int], retried: Counter[str]) -> dict[str, Any]:
    return {
        "items": items,
        "wall_s": round(wall, 3),
        "items_per_s": round(items / wall, 2) if wall else None,
        "errors": errors,
        "retried": dict(retried),
    }


def _merge_rounds(rounds: list[dict[str, Any]]) -> dict[str, Any]:
    """Sum the pooled stats of the SOFT rounds (targets, then SubSeries)."""
    wall = sum(r["wall_s"] for r in rounds)
    items = sum(r["items"] for r in rounds)
    wire = sum(r["wire_bytes"] for r in rounds)
    events: Counter[str] = Counter()
    for r in rounds:
        events.update(r["throttle_events"])
    return {
        "rounds": len(rounds),
        "connections": rounds[0]["connections"],
        "items": items,
        "missing_404": [k for r in rounds for k in r["missing_404"]],
        "failed": {k: v for r in rounds for k, v in r["failed"].items()},
        "wire_bytes": wire,
        "wall_s": round(wall, 3),
        "items_per_s": round(items / wall, 2) if wall else None,
        "bytes_per_s": round(wire / wall) if wall else None,
        "item_ms": rounds[0]["item_ms"],
        "throttle_events": dict(events),
        "connections_at_end": min(r["connections_at_end"] for r in rounds),
    }


def tab_links(root: Path, gsms: set[str]) -> list[tuple[str, str, str]]:
    """Return (GSM, SRX, BioProject) for the GEO experiments ``SRA_Accessions.tab`` lists.

    An experiment counts when its center is GEO and its alias is one of ``gsms``.
    """
    if not gsms:
        return []
    tab = snapshots.snapshot(root, snapshots.SRA_ACCESSIONS).path
    listed = ", ".join(extract.sql_string(g) for g in sorted(gsms))
    with extract.connect(root) as con:
        rows = con.execute(
            "SELECT Alias, Accession, BioProject FROM read_csv("
            f"{extract.sql_string(str(tab))}, delim='\t', header=true, all_varchar=true, "
            "quote='') WHERE Type = 'EXPERIMENT' AND Center = 'GEO' "
            f"AND Alias IN ({listed})"
        ).fetchall()
    return [(str(a), str(x), "" if p in (None, "-") else str(p)) for a, x, p in rows]


def elink_extra(pilot: Path, step: str) -> dict[str, Any]:
    """Count the links ``elink`` adds beyond SOFT relation lines and ``SRA_Accessions.tab``.

    Sample to SRX: SOFT ``!Sample_relation = SRA`` lines, else a tab experiment whose alias
    is the GSM. Series to BioProject: SOFT ``!Series_relation = BioProject`` lines, else the
    BioProject of a tab experiment under one of the Series' samples. Writes
    ``elink-extra.json`` and ``elink-extra.tsv`` (per Series).
    """
    out = pilot_dir(pilot, step)
    held = sorted(raw.saved(pilot, raw.GEO_SOFT, read_targets(pilot, step)))
    soft = raw.saved(pilot, raw.GEO_SOFT, held)
    linked = raw.saved(pilot, raw.ENTREZ_LINKS, held)
    # What each Series' SOFT and the tab already state, keyed by Series: the saved elink
    # lines name the Series, not the sample, as their source.
    stated: dict[str, set[tuple[str, str]]] = defaultdict(set)
    gsm_series: dict[str, list[str]] = defaultdict(list)
    for gse in held:
        for entity, gsm, _ in sources.soft_blocks(soft[gse]):
            if entity == "SAMPLE":
                gsm_series[gsm].append(gse)
        try:
            _, rows = sources.soft(soft[gse])
        except Exception:
            rows = []
        stated[gse] |= {(r.to, r.relation) for r in rows if r.relation in ("SRA", "BioProject")}
    say(f"elink-extra: scanning SRA_Accessions.tab for {len(gsm_series)} samples")
    start = time.perf_counter()
    tab = tab_links(pilot, set(gsm_series))
    tab_s = round(time.perf_counter() - start, 3)
    say(f"elink-extra: {len(tab)} tab rows in {tab_s} s")
    for gsm, srx, project in tab:
        for gse in gsm_series[gsm]:
            stated[gse].add((srx, "SRA"))
            if project:
                stated[gse].add((project, "BioProject"))
    per_series: dict[str, Counter[str]] = {}
    for gse in held:
        counts: Counter[str] = Counter()
        for row in sources.entrez_links(linked.get(gse, "")):
            key = "sample_to_srx" if row.relation == "SRA" else "series_to_bioproject"
            counts[f"{key}_elink"] += 1
            if (row.to, row.relation) not in stated[gse]:
                counts[f"{key}_extra"] += 1
        per_series[gse] = counts
    total: Counter[str] = Counter()
    for counts in per_series.values():
        total.update(counts)
    keys = (
        "sample_to_srx_elink",
        "sample_to_srx_extra",
        "series_to_bioproject_elink",
        "series_to_bioproject_extra",
    )
    write_atomic(
        out / "elink-extra.tsv",
        "accession\t"
        + "\t".join(keys)
        + "\n"
        + "".join(
            f"{g}\t" + "\t".join(str(c[k]) for k in keys) + "\n" for g, c in per_series.items()
        ),
    )
    result = {
        "series": len(held),
        "total": {k: total[k] for k in keys},
        "series_with_extra": sum(
            1
            for c in per_series.values()
            if c["sample_to_srx_extra"] or c["series_to_bioproject_extra"]
        ),
        "tab_rows": len(tab),
        "tab_scan_s": tab_s,
    }
    write_atomic(out / "elink-extra.json", json.dumps(result, indent=2) + "\n")
    return result


# ---------- build ----------


def run_build(pilot: Path, build_id: str) -> dict[str, Any]:
    """Build once into ``builds/<build_id>/`` and record its wall time and peak memory."""
    path = pilot_dir(pilot) / f"build-{build_id}.json"
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    start = time.perf_counter()
    out = build.build(pilot, build_id)
    result = {
        "build_id": build_id,
        "wall_s": round(time.perf_counter() - start, 3),
        "maxrss_bytes": peak_rss(),
        "bytes": sum(p.stat().st_size for p in out.rglob("*") if p.is_file()),
        "rows": {
            str(p.relative_to(out)): pq.ParquetFile(p).metadata.num_rows
            for p in sorted(out.rglob("*.parquet"))
        },
        "job": os.environ.get("SLURM_JOB_ID"),
        "cpus": os.environ.get("SLURM_CPUS_PER_TASK"),
        "mem_mb": os.environ.get("SLURM_MEM_PER_NODE"),
    }
    write_atomic(path, json.dumps(result, indent=2) + "\n")
    return result


# ---------- report ----------


def quantiles(values: list[int]) -> dict[str, int]:
    """Return the count, p50, p90, p99 and max of ``values`` (nearest rank)."""
    if not values:
        return {"n": 0}
    ordered = sorted(values)

    def rank(q: float) -> int:
        return ordered[min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))]

    return {
        "n": len(ordered),
        "p50": rank(0.5),
        "p90": rank(0.9),
        "p99": rank(0.99),
        "max": ordered[-1],
    }


def source_bytes(pilot: Path) -> dict[str, Any]:
    """Return rows, text bytes and Parquet bytes per raw source in the pilot root."""
    out: dict[str, Any] = {}
    for source in _RAW_SOURCES:
        folder = pilot / "raw" / source
        buckets = sorted(folder.glob("*.parquet")) if folder.is_dir() else []
        sizes = [
            len(str(text).encode("utf-8"))
            for p in buckets
            for text in pq.read_table(p, columns=["raw_text"]).column("raw_text").to_pylist()
        ]
        out[source] = {
            "rows": len(sizes),
            "text_bytes": sum(sizes),
            "parquet_bytes": sum(p.stat().st_size for p in buckets),
            "buckets": len(buckets),
            "row_text_bytes": quantiles(sizes),
            "bucket_parquet_bytes": quantiles([p.stat().st_size for p in buckets]),
        }
    return out


def summarize_fetch(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """Total a stage 1 fetch log: wall time per source, failures by kind, retries."""
    steps: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"calls": 0, "seconds": 0.0, "bytes": 0, "errors": Counter()}
    )
    failures: Counter[str] = Counter()
    retried: Counter[str] = Counter()
    skipped: dict[str, str] = {}
    wall = 0.0
    for entry in entries:
        wall += entry["wall_s"]
        for name, s in entry["steps"].items():
            steps[name]["calls"] += s["calls"]
            steps[name]["seconds"] += s["seconds"]
            steps[name]["bytes"] += s["bytes"]
            steps[name]["errors"].update(s["errors"])
        for f in entry["failed"].values():
            failures[f"{f['step']}: {f['error']}"] += 1
        skipped |= entry["soft_type_skipped"]
        retried.update(entry.get("retried", {}))
    logged = {gse for entry in entries for gse in entry["series"]}
    return {
        "series": len(logged),
        "wall_s": round(wall, 3),
        "s_per_series": round(wall / len(logged), 3) if logged else None,
        "steps": {
            k: v | {"seconds": round(v["seconds"], 3), "errors": dict(v["errors"])}
            for k, v in sorted(steps.items())
        },
        "retried": dict(retried),
        "failures_by_kind": dict(failures.most_common()),
        "soft_type_skipped": len(skipped),
        "restarts": sorted({str(e.get("restart")) for e in entries}),
        "peak_rss_bytes": max((e.get("maxrss_bytes", 0) for e in entries), default=0),
    }


def stability(c: dict[str, Any]) -> dict[str, Any]:
    """Judge a step stable: no 429 or 503 and timeouts under 1% of requests, per source."""
    if not c:
        return {"stable": None}
    throttled: dict[str, int] = {}
    timeouts: dict[str, float] = {}
    for source in _FETCHED:
        s = c[source]
        events = s.get("throttle_events", {}) | s.get("retried", {})
        throttled |= {f"{source} {k}": v for k, v in events.items() if "429" in k or "503" in k}
        timed = sum(v for k, v in events.items() if "imeout" in k)
        timeouts[source] = round(timed / max(1, s["items"]), 4)
    return {
        "throttled": throttled,
        "timeout_share": timeouts,
        "stable": not throttled and all(v < 0.01 for v in timeouts.values()),
    }


def projections(
    c: dict[str, Any], shards: dict[str, Any], drawn: int, total_series: int, total_exp: int
) -> dict[str, Any]:
    """Return the hours each source would take for the whole set at this step's rate."""
    out: dict[str, Any] = {"total_series": total_series, "total_experiments": total_exp}
    scale = total_series / drawn if drawn else 0.0
    if c:
        for source in _FETCHED:
            s = c[source]
            if source == raw.SRA_XML:
                rate = s["items_per_s"]
                out[source] = round(total_exp / rate / 3600, 2) if rate else None
            else:
                out[source] = round(s["wall_s"] * scale / 3600, 2)
        out["all_sources_in_turn"] = round(sum(v for k, v in out.items() if k in _FETCHED and v), 2)
    if shards.get("s_per_series"):
        out["stage1_serial"] = round(shards["s_per_series"] * total_series / 3600, 2)
    return out


def report(pilot: Path, step: str, total_experiments: int = DEFAULT_EXPERIMENTS) -> dict[str, Any]:
    """Write a step's ``results.json`` and ``results.md``."""
    out = pilot_dir(pilot, step)
    meta = json.loads((out / "targets.json").read_text(encoding="utf-8"))
    concurrent = _load(out / "concurrent.json")
    logs = fetch_logs(pilot, step)
    shards = summarize_fetch([e for entries in logs.values() for e in entries])
    shards["per_shard"] = {k: summarize_fetch(v) for k, v in logs.items()}
    results: dict[str, Any] = {
        "step": step,
        "targets": meta,
        "concurrent": concurrent,
        "stage1_shards": shards,
        "elink_extra": _load(out / "elink-extra.json"),
        "stability": stability(concurrent),
        "projection_hours": projections(
            concurrent, shards, meta["drawn"], meta["sequencing_series"], total_experiments
        ),
        "bytes": source_bytes(pilot),
        "builds": [
            json.loads(p.read_text(encoding="utf-8"))
            for p in sorted(pilot_dir(pilot).glob("build-*.json"))
        ],
        "commit": _commit(),
    }
    write_atomic(out / "results.json", json.dumps(results, indent=2) + "\n")
    write_atomic(out / "results.md", render(results))
    return results


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def _commit() -> str | None:
    return provenance.produced_by("scripts/store/pilot_stage2.py").get("commit")


def render(r: dict[str, Any]) -> str:
    """Return a step's results as a short Markdown page; ``results.json`` holds everything."""
    t, c, s, p = r["targets"], r["concurrent"], r["stability"], r["projection_hours"]
    lines = [
        f"# Stage 2 pilot: {r['step']}",
        "",
        f"Written by `scripts/store/pilot_stage2.py` at commit `{r['commit']}`.",
        f"Targets: {t['drawn']} of {t['sequencing_series']} sequencing Series "
        f"({t['drawn_mixed']} mixed) in the {t['snapshot_date']} `geo-series` snapshot, "
        f"seed {t['seed']}.",
        f"Stable: {s['stable']} (429/503: {s.get('throttled') or 'none'}; "
        f"timeout share: {s.get('timeout_share')}).",
        "",
    ]
    if c:
        lines += [
            f"## Concurrent fetch: {c['connections']} connections, {c['inflight']} in flight",
            "",
            "| Source | Items | Wall s | Items/s | Bytes/s | Errors and retries "
            "| Whole set, h (inferred) |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for source in _FETCHED:
            v = c[source]
            errors = v.get("throttle_events", {}) | v.get("retried", {}) | v.get("errors", {})
            if v.get("failed"):
                errors |= {"failed": len(v["failed"])}
            if v.get("missing_404"):
                errors |= {"404": len(v["missing_404"])}
            if v.get("missing"):
                errors |= {"missing": v["missing"]}
            lines.append(
                f"| {source} | {v['items']} | {v['wall_s']:.0f} | {v['items_per_s']} | "
                f"{v.get('bytes_per_s', '')} | {errors or ''} | {p.get(source)} |"
            )
        lines += [
            "",
            f"All sources one after another, whole set: {p.get('all_sources_in_turn')} h "
            f"(inferred). Pack (single writer of `raw/`): {c['pack']['wall_s']:.0f} s. "
            f"Original and stripped SOFT sizes: `soft-original.tsv`.",
            "",
        ]
    eu = c.get("eutils") if c else None
    if eu:
        lines += [
            f"## E-utilities: {c.get('rate')} requests/s asked, {eu['rate_at_end']} at the end",
            "",
            "| Utility | Requests | IDs per request p50 / max | Req/s | Latency ms p50 / p95 "
            "| Throttles | Retries |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for util, v in eu.items():
            if util == "rate_at_end":
                continue
            ids, ms = v["ids_per_request"], v["latency_ms"]
            lines.append(
                f"| {util} | {v['requests']} | {ids.get('p50')} / {ids.get('max')} | "
                f"{v['req_per_s']} | {ms.get('p50')} / {ms.get('p95')} | "
                f"{v['throttles'] or ''} | {v['retries'] or ''} |"
            )
        lines.append("")
    e = r["elink_extra"]
    if e:
        lines += [
            "## What elink adds",
            "",
            f"Beyond SOFT relation lines and `SRA_Accessions.tab`, over {e['series']} Series: "
            f"{e['total']['sample_to_srx_extra']} of {e['total']['sample_to_srx_elink']} "
            f"sample-to-SRX links and {e['total']['series_to_bioproject_extra']} of "
            f"{e['total']['series_to_bioproject_elink']} Series-to-BioProject links, in "
            f"{e['series_with_extra']} Series. Per Series: `elink-extra.tsv`.",
            "",
        ]
    sh = r["stage1_shards"]
    if sh["series"]:
        lines += [
            f"## Serial stage 1 fetch, {len(sh['per_shard'])} shards",
            "",
            f"{sh['s_per_series']} s per Series; the whole set takes {p.get('stage1_serial')} "
            "h on one process (inferred).",
            "",
            "| Shard | Series | Wall s | Retried | Failures |",
            "| --- | --- | --- | --- | --- |",
        ]
        lines += [
            f"| {k} | {v['series']} | {v['wall_s']:.0f} | {v['retried'] or ''} | "
            f"{v['failures_by_kind'] or ''} |"
            for k, v in sh["per_shard"].items()
        ]
        lines.append("")
    lines += [
        "## Bytes in the pilot's raw/",
        "",
        "| Source | Rows | Text bytes | Parquet bytes | Row p50 / p99 / max |",
        "| --- | --- | --- | --- | --- |",
    ]
    for k, v in r["bytes"].items():
        q = v["row_text_bytes"]
        spread = f"{q.get('p50', 0)} / {q.get('p99', 0)} / {q.get('max', 0)}"
        lines.append(f"| {k} | {v['rows']} | {v['text_bytes']} | {v['parquet_bytes']} | {spread} |")
    lines += ["", "## Builds", "", "| Build | Wall s | Peak RSS bytes | Bytes |"]
    lines += ["| --- | --- | --- | --- |"]
    lines += [
        f"| {b['build_id']} | {b['wall_s']:.0f} | {b['maxrss_bytes']} | {b['bytes']} |"
        for b in r["builds"]
    ]
    return "\n".join(lines) + "\n"


# ---------- command line ----------


def main(argv: list[str] | None = None) -> int:
    """Run one pilot command."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("setup")
    p.add_argument("--main", type=Path, required=True, help="the main store root, read only")
    p.add_argument("--pilot", type=Path, required=True)
    p = sub.add_parser("draw")
    p.add_argument("--pilot", type=Path, required=True)
    p.add_argument("--name", required=True, help="the step's folder under <pilot>/pilot/")
    how = p.add_mutually_exclusive_group(required=True)
    how.add_argument("--fraction", type=float, help="the share of sequencing Series to take")
    how.add_argument("--size", type=int, help="the number of sequencing Series to take")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p = sub.add_parser("concurrent")
    p.add_argument("--pilot", type=Path, required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--connections", type=int, required=True, help="HTTPS connections to GEO")
    p.add_argument("--inflight", type=int, required=True, help="E-utilities calls at once")
    p.add_argument("--batch", type=int, default=300, help="SRA experiments per efetch")
    p.add_argument("--rate", type=float, default=DEFAULT_RATE, help="E-utilities requests/s")
    p = sub.add_parser("elink-extra")
    p.add_argument("--pilot", type=Path, required=True)
    p.add_argument("--name", required=True)
    p = sub.add_parser("shard")
    p.add_argument("--pilot", type=Path, required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--count", type=int, required=True)
    p.add_argument("--index", type=int, default=int(os.environ.get("SLURM_ARRAY_TASK_ID", "0")))
    p = sub.add_parser("build")
    p.add_argument("--pilot", type=Path, required=True)
    p.add_argument("--build-id", required=True)
    p = sub.add_parser("report")
    p.add_argument("--pilot", type=Path, required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--total-experiments", type=int, default=DEFAULT_EXPERIMENTS)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING)
    if args.command == "setup":
        setup(args.main, args.pilot)
    elif args.command == "draw":
        meta = draw_step(args.pilot, args.name, args.fraction or 0.0, args.seed, size=args.size)
        print(json.dumps(meta))
    elif args.command == "concurrent":
        result = concurrent_fetch(
            args.pilot,
            args.name,
            EUtils(
                email=(creds := load_credentials(allow_prompt=False)).email,
                api_key=creds.api_key,
                rate=args.rate if creds.api_key else min(args.rate, 2.5),
                inflight=args.inflight,
            ),
            connections=args.connections,
            inflight=args.inflight,
            batch=args.batch,
        )
        print(json.dumps({k: v for k, v in result.items() if k in ("wall_s", "pack")}))
    elif args.command == "elink-extra":
        print(json.dumps(elink_extra(args.pilot, args.name)))
    elif args.command == "shard":
        print(
            run_shard(
                args.pilot, args.name, args.index, args.count, EntrezClient(allow_prompt=False)
            )
        )
    elif args.command == "build":
        print(json.dumps(run_build(args.pilot, args.build_id)))
    else:
        report(args.pilot, args.name, args.total_experiments)
        print(pilot_dir(args.pilot, args.name) / "results.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
