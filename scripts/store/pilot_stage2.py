"""Measure the stage 2 fetch and the stage 1 build on seeded samples of GEO sequencing Series.

The pilot runs in its own store root and never writes the main store. ``setup`` makes the
pilot root and links the main store's ``snapshots/`` and ``work/extract/`` into it. Each
``step`` draws a seeded, nested fraction of the sequencing Series, then:

1. refetches their family SOFT over a few kept-open connections, saving nothing;
2. runs the stage 1 fetch, :func:`labdata.store.fetch.fetch`, into ``raw/``, logging each
   source's time, bytes and failures;
3. refetches every saved SRA experiment with several ``efetch`` calls in flight, saving
   nothing;
4. writes ``results.json`` and ``results.md``: speed per source, errors by kind, whether the
   step was stable, and the projected time for the whole set.

``build`` times one build and its peak memory. A step's files go under
``<pilot>/pilot/<step>/``, and a killed step resumes.

Usage, each under ``pixi run python scripts/store/``::

    pilot_stage2.py setup --main MAIN --pilot P
    pilot_stage2.py step --pilot P --name step1 --fraction 0.001 --soft-connections 2 --sra-inflight 2
    pilot_stage2.py build --pilot P --build-id pilot-1
    pilot_stage2.py report --pilot P --name step1
"""

from __future__ import annotations

import argparse
import gzip
import http.client
import json
import logging
import math
import os
import queue
import random
import resource
import sys
import threading
import time
import urllib.parse
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from labdata.geo.sra_records import split_packages
from labdata.ncbi import entrez
from labdata.ncbi.entrez import EntrezClient
from labdata.store import build, fetch, init_store, raw, snapshots
from labdata.store._files import write_atomic
from labdata.store.root import MARKER

SEQUENCING = "high throughput sequencing"
DEFAULT_FRACTION = 0.01
DEFAULT_SEED = 50
DEFAULT_CHUNK = 50
#: The GEO-linked SRA experiment count the SRA projection scales to; pass the current count.
DEFAULT_EXPERIMENTS = 5_640_000
_TABLE_BEGIN = ("!sample_table_begin", "!platform_table_begin")
_TABLE_END = ("!sample_table_end", "!platform_table_end")
_THROTTLED = (429, 503)
_USER_AGENT = "liulab-data (+https://github.com/liuhlab/liulab-data)"
_RAW_SOURCES = (raw.GEO_SOFT, raw.GEO_FILELIST, raw.ENTREZ_LINKS, raw.SRA_XML, raw.PUBMED_SI)


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


def draw(pool: list[Target], fraction: float, seed: int) -> list[Target]:
    """Return a seeded random ``fraction`` of ``pool`` (at least one), sorted by number.

    The draw is a prefix of one seeded shuffle, so a smaller fraction's draw lies inside a
    larger one's.
    """
    k = max(1, round(len(pool) * fraction))
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

    def _call(self, step: str, fn: Any, *args: Any, **kwargs: Any) -> Any:
        stats = self.stats[step]
        stats.calls += 1
        start = time.perf_counter()
        try:
            out = fn(*args, **kwargs)
        except Exception as exc:
            stats.errors[type(exc).__name__] += 1
            self.last_failed = step
            raise
        finally:
            stats.seconds += time.perf_counter() - start
        stats.bytes += _size(out)
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


def _size(out: object) -> int:
    if isinstance(out, str):
        return len(out.encode("utf-8"))
    return 0


class _SkipLog(logging.Handler):
    """Collect the Series the stage 1 fetch skips because their SOFT type is not sequencing."""

    def __init__(self) -> None:
        super().__init__(logging.INFO)
        self.skipped: dict[str, str] = {}

    def emit(self, record: logging.LogRecord) -> None:
        if str(record.msg).startswith("skipped") and isinstance(record.args, tuple):
            self.skipped[str(record.args[0])] = str(record.args[1])


# ---------- steps ----------


def pilot_dir(pilot: Path, step: str | None = None) -> Path:
    """Return the folder of the pilot's own files, or of one step's targets, logs and results."""
    return pilot / "pilot" if step is None else pilot / "pilot" / step


def setup(main: Path, pilot: Path) -> None:
    """Make the pilot store and link the main store's snapshots and extracts into it."""
    if main.resolve() == pilot.resolve():
        raise SystemExit("the pilot root must differ from the main store root")
    if not (main / MARKER).is_file():
        raise SystemExit(f"{main} is not a labdata store: it has no {MARKER}")
    if not (pilot / MARKER).is_file():
        init_store(pilot)
    (pilot / "work").mkdir(exist_ok=True)
    for link, target in (
        (pilot / "snapshots", main / "snapshots"),
        (pilot / "work" / "extract", main / "work" / "extract"),
    ):
        if not link.is_symlink():
            link.symlink_to(target.resolve(), target_is_directory=True)
        elif link.resolve() != target.resolve():
            raise SystemExit(f"{link} points at {link.resolve()}, not {target.resolve()}")
    pilot_dir(pilot).mkdir(exist_ok=True)


def draw_step(pilot: Path, step: str, fraction: float, seed: int, chunk: int) -> dict[str, Any]:
    """Draw one step's targets into its folder; a rerun with other settings is refused."""
    out = pilot_dir(pilot, step)
    out.mkdir(parents=True, exist_ok=True)
    meta_path = out / "targets.json"
    if meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        asked = {"fraction": fraction, "seed": seed, "chunk": chunk}
        if any(meta[k] != v for k, v in asked.items()):
            raise SystemExit(f"{meta_path} was drawn with other settings: {meta}")
        return meta
    snap = snapshots.snapshot(pilot, snapshots.GEO_SERIES)
    with gzip.open(snap.path, "rt", encoding="utf-8") as fh:
        pool = sequencing_series(fh)
    chosen = draw(pool, fraction, seed)
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


def run_fetch(pilot: Path, step: str, client: EntrezClient) -> Path:
    """Fetch every target chunk not yet logged, appending one log line per chunk."""
    meta = json.loads((pilot_dir(pilot, step) / "targets.json").read_text(encoding="utf-8"))
    log_path = pilot_dir(pilot, step) / "fetch-log.jsonl"
    done = {entry["chunk"] for entry in read_log(log_path)}
    skips = _SkipLog()
    logger = logging.getLogger(fetch.__name__)
    logger.addHandler(skips)
    logger.setLevel(logging.INFO)
    for index, chunk in enumerate(chunks(sorted(read_targets(pilot, step)), meta["chunk"])):
        if index in done:
            continue
        metered = MeteredClient(client)
        retried = TRANSIENT.copy()
        skips.skipped = {}
        failed: dict[str, dict[str, str]] = {}
        start = time.perf_counter()
        try:
            fetch.fetch(pilot, chunk, client=metered)
        except Exception:
            # Fetch the chunk Series by Series so each failure is pinned to one Series.
            for gse in chunk:
                metered.last_failed = None
                try:
                    fetch.fetch(pilot, [gse], client=metered)
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
                step: {
                    "calls": s.calls,
                    "seconds": round(s.seconds, 3),
                    "bytes": s.bytes,
                    "errors": dict(s.errors),
                }
                for step, s in sorted(metered.stats.items())
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


def peak_rss() -> int:
    """Return this process's peak resident memory in bytes."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024


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


def sra_bench(
    pilot: Path,
    step: str,
    client: EntrezClient,
    *,
    inflight: int,
    batch: int,
    n: int | None = None,
    seed: int = DEFAULT_SEED,
) -> dict[str, Any]:
    """Time ``efetch`` of the fetched experiments in batches of ``batch``, ``inflight`` at once.

    The experiments are those in ``raw/sra-xml``, or ``n`` of them drawn with ``seed``.
    Replies are counted and discarded. A package left out counts as missing; a failed call
    counts its whole batch as missing.
    """
    path = pilot_dir(pilot, step) / "sra-bench.json"
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    folder = pilot / "raw" / raw.SRA_XML
    pool = sorted(
        str(a)
        for p in sorted(folder.glob("*.parquet"))
        for a in pq.read_table(p, columns=["accession"]).column("accession").to_pylist()
    )
    sample = pool if n is None else sorted(random.Random(seed).sample(pool, min(n, len(pool))))
    retried = TRANSIENT.copy()

    def one(ids: list[str]) -> tuple[int, int, float, str | None]:
        start = time.perf_counter()
        try:
            reply = client.efetch("sra", ids, rettype="full", retmode="xml", refresh=True)
        except Exception as exc:
            return 0, 0, time.perf_counter() - start, type(exc).__name__
        got = {a: t for a, t in split_packages(reply).items() if a in set(ids)}
        size = sum(len(t.encode("utf-8")) for t in got.values())
        return len(got), size, time.perf_counter() - start, None

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=inflight) as workers:
        replies = list(workers.map(one, chunks(sample, batch)))
    wall = time.perf_counter() - start
    client.clear_cache()
    packages = sum(r[0] for r in replies)
    size = sum(r[1] for r in replies)
    credentials = getattr(client, "credentials", None)
    result = {
        "inflight": inflight,
        "batch": batch,
        "requested": len(sample),
        "packages": packages,
        "missing": len(sample) - packages,
        "failed_calls": dict(Counter(r[3] for r in replies if r[3])),
        "calls": len(replies),
        "wall_s": round(wall, 3),
        "call_ms": quantiles([round(r[2] * 1000) for r in replies]),
        "retried": dict(TRANSIENT - retried),
        "bytes": size,
        "packages_per_s": round(packages / wall, 2) if wall else None,
        "bytes_per_s": round(size / wall) if wall else None,
        "s_per_100_packages": round(wall / packages * 100, 3) if packages else None,
        "bytes_per_package": round(size / packages) if packages else None,
        "api_key": bool(credentials is not None and credentials.api_key),
    }
    write_atomic(path, json.dumps(result, indent=2) + "\n")
    return result


class _ThrottledError(Exception):
    """The server answered 429 or 503."""


def soft_bench(
    pilot: Path,
    step: str,
    *,
    connections: int,
    base: str = "https://ftp.ncbi.nlm.nih.gov",
    timeout: float = 60.0,
    tries: int = 3,
) -> dict[str, Any]:
    """Refetch a step's gzipped family SOFT over ``connections`` kept-open connections.

    Each worker keeps one connection open between files and streams each body, counting
    bytes and discarding them. A 429, 503 or timeout halves the connections in use, and the
    file is tried again later, up to ``tries`` times. Nothing is saved to ``raw/``.
    """
    path = pilot_dir(pilot, step) / "soft-bench.json"
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    url = urllib.parse.urlsplit(base)
    targets = sorted(read_targets(pilot, step))
    todo: queue.Queue[str] = queue.Queue()
    for gse in targets:
        todo.put(gse)
    lock = threading.Lock()
    limit = [connections]
    attempts: Counter[str] = Counter()
    events: Counter[str] = Counter()
    gz_bytes: dict[str, int] = {}
    seconds: list[int] = []
    missing: list[str] = []
    errors: dict[str, str] = {}

    def connect() -> http.client.HTTPConnection:
        if url.scheme == "https":
            return http.client.HTTPSConnection(url.netloc, timeout=timeout)
        return http.client.HTTPConnection(url.netloc, timeout=timeout)

    def get(conn: http.client.HTTPConnection, gse: str) -> int | None:
        target = urllib.parse.urlsplit(fetch.soft_url(gse))
        conn.request("GET", f"{url.path}{target.path}", headers={"User-Agent": _USER_AGENT})
        resp = conn.getresponse()
        if resp.status != 200:
            resp.read()
            if resp.status in _THROTTLED:
                raise _ThrottledError(str(resp.status))
            if resp.status == 404:
                return None
            raise http.client.HTTPException(f"HTTP {resp.status}")
        size = 0
        while block := resp.read(1 << 20):
            size += len(block)
        return size

    def worker(index: int) -> None:
        conn: http.client.HTTPConnection | None = None
        while index < limit[0]:
            try:
                gse = todo.get_nowait()
            except queue.Empty:
                break
            attempts[gse] += 1
            start = time.perf_counter()
            try:
                conn = conn or connect()
                size = get(conn, gse)
            except (_ThrottledError, TimeoutError) as exc:
                kind = f"HTTP {exc}" if isinstance(exc, _ThrottledError) else "timeout"
                with lock:
                    events[kind] += 1
                    limit[0] = max(1, limit[0] // 2)
                if conn:
                    conn.close()
                conn = None
                if attempts[gse] < tries:
                    todo.put(gse)
                else:
                    errors[gse] = kind
                time.sleep(min(30.0, 2.0 ** attempts[gse]))
                continue
            except (OSError, http.client.HTTPException) as exc:
                if conn:
                    conn.close()
                conn = None
                if attempts[gse] < tries:
                    todo.put(gse)
                else:
                    errors[gse] = f"{type(exc).__name__}: {exc}"[:200]
                continue
            seconds.append(round((time.perf_counter() - start) * 1000))
            if size is None:
                missing.append(gse)
            else:
                gz_bytes[gse] = size
        if conn:
            conn.close()

    start = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(connections)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    worker(0)  # a file put back by a worker that then stopped
    wall = time.perf_counter() - start
    write_atomic(
        pilot_dir(pilot, step) / "soft-gz-sizes.tsv",
        "accession\tgz_bytes\n" + "".join(f"{g}\t{n}\n" for g, n in sorted(gz_bytes.items())),
    )
    total = sum(gz_bytes.values())
    result = {
        "connections": connections,
        "files": len(gz_bytes),
        "missing_404": sorted(missing),
        "failed": errors,
        "gz_bytes": total,
        "gz_file_bytes": quantiles(list(gz_bytes.values())),
        "wall_s": round(wall, 3),
        "files_per_s": round(len(gz_bytes) / wall, 2) if wall else None,
        "bytes_per_s": round(total / wall) if wall else None,
        "file_ms": quantiles(seconds),
        "throttle_events": dict(events),
        "connections_at_end": limit[0],
    }
    write_atomic(path, json.dumps(result, indent=2) + "\n")
    return result


# ---------- report ----------


def table_bytes(text: str) -> int:
    """Return the UTF-8 bytes of a family SOFT's sample and platform data table blocks.

    The begin and end marker lines count as part of a block.
    """
    total = 0
    inside = False
    for line in text.splitlines(keepends=True):
        lower = line.lower()
        if lower.startswith(_TABLE_BEGIN):
            inside = True
        if inside:
            total += len(line.encode("utf-8"))
        if lower.startswith(_TABLE_END):
            inside = False
    return total


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


def _rows(folder: Path) -> Iterator[tuple[Path, str, str]]:
    for path in sorted(folder.glob("*.parquet")):
        table = pq.read_table(path, columns=["accession", "raw_text"]).to_pydict()
        for accession, text in zip(table["accession"], table["raw_text"], strict=True):
            yield path, accession, text


def source_bytes(pilot: Path, step: str, targets: dict[str, bool]) -> dict[str, Any]:
    """Return rows, text bytes and Parquet bytes per raw source, with SOFT split by kind."""
    out: dict[str, Any] = {}
    for source in _RAW_SOURCES:
        folder = pilot / "raw" / source
        buckets = sorted(folder.glob("*.parquet")) if folder.is_dir() else []
        sizes: list[int] = []
        soft: dict[str, list[int]] = defaultdict(list)
        stripped: list[int] = []
        per_file: list[str] = []
        for _, accession, text in _rows(folder):
            n = len(text.encode("utf-8"))
            sizes.append(n)
            if source == raw.GEO_SOFT:
                kind = "mixed" if targets.get(accession) else "sequencing-only"
                kind = kind if accession in targets else "not-a-target"
                soft[kind].append(n)
                stripped.append(n - table_bytes(text))
                per_file.append(f"{accession}\t{kind}\t{n}\t{stripped[-1]}\n")
        entry: dict[str, Any] = {
            "rows": len(sizes),
            "text_bytes": sum(sizes),
            "parquet_bytes": sum(p.stat().st_size for p in buckets),
            "buckets": len(buckets),
            "row_text_bytes": quantiles(sizes),
            "bucket_parquet_bytes": quantiles([p.stat().st_size for p in buckets]),
        }
        if source == raw.GEO_SOFT:
            entry["by_kind"] = {k: quantiles(v) | {"sum": sum(v)} for k, v in sorted(soft.items())}
            entry["stripped_text_bytes"] = sum(stripped)
            entry["row_stripped_bytes"] = quantiles(stripped)
            write_atomic(
                pilot_dir(pilot, step) / "soft-sizes.tsv",
                "accession\tkind\tbytes\tstripped_bytes\n" + "".join(per_file),
            )
        out[source] = entry
    return out


def summarize_fetch(entries: list[dict[str, Any]], targets: dict[str, bool]) -> dict[str, Any]:
    """Total the fetch log: wall time per step, failures by kind and SOFT-type skips."""
    steps: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"calls": 0, "seconds": 0.0, "bytes": 0, "errors": Counter()}
    )
    failures: Counter[str] = Counter()
    examples: dict[str, list[str]] = defaultdict(list)
    skipped: dict[str, str] = {}
    retried: Counter[str] = Counter()
    wall = 0.0
    for entry in entries:
        wall += entry["wall_s"]
        for step, s in entry["steps"].items():
            steps[step]["calls"] += s["calls"]
            steps[step]["seconds"] += s["seconds"]
            steps[step]["bytes"] += s["bytes"]
            steps[step]["errors"].update(s["errors"])
        for gse, f in entry["failed"].items():
            kind = f"{f['step']}: {f['error']}"
            failures[kind] += 1
            if len(examples[kind]) < 5:
                examples[kind].append(f"{gse}: {f['message']}")
        skipped |= entry["soft_type_skipped"]
        retried.update(entry.get("retried", {}))
    logged = {gse for entry in entries for gse in entry["series"]}
    failed = {gse for entry in entries for gse in entry["failed"]}
    network = sum(s["seconds"] for s in steps.values())
    return {
        "chunks": len(entries),
        "series_logged": len(logged),
        "series_not_logged": len(set(targets) - logged),
        "wall_s": round(wall, 3),
        "steps": {
            k: v | {"seconds": round(v["seconds"], 3), "errors": dict(v["errors"])}
            for k, v in sorted(steps.items())
        },
        "other_s": round(wall - network, 3),
        "retried": dict(retried),
        "failed_series": len(failed),
        "failures_by_kind": dict(failures.most_common()),
        "failure_examples": dict(examples),
        "soft_type_skipped": len(skipped.keys() & targets.keys()),
        "soft_type_skipped_types": dict(Counter(skipped[g] for g in skipped).most_common()),
        "restarts": sorted({str(e.get("restart")) for e in entries}),
        "peak_rss_bytes": max((e.get("maxrss_bytes", 0) for e in entries), default=0),
    }


def report(pilot: Path, step: str, total_experiments: int = DEFAULT_EXPERIMENTS) -> dict[str, Any]:
    """Write a step's ``results.json`` and ``results.md`` from its logs, ``raw/`` and builds."""
    out = pilot_dir(pilot, step)
    meta = json.loads((out / "targets.json").read_text(encoding="utf-8"))
    targets = read_targets(pilot, step)
    fetched = summarize_fetch(read_log(out / "fetch-log.jsonl"), targets)
    held = set(raw.saved(pilot, raw.GEO_SOFT, targets))
    results: dict[str, Any] = {
        "step": step,
        "targets": meta,
        "fetch": fetched,
        "held_target_series": len(held),
        "gaps": len(targets) - len(held),
        "bytes": source_bytes(pilot, step, targets),
        "soft_serial": _soft_serial(fetched),
        "soft_bench": _load(out / "soft-bench.json"),
        "sra_serial": _sra_serial(fetched, pilot),
        "sra_bench": _load(out / "sra-bench.json"),
        "builds": [
            json.loads(p.read_text(encoding="utf-8"))
            for p in sorted(pilot_dir(pilot).glob("build-*.json"))
        ],
        "commit": _commit(),
    }
    results["stability"] = stability(results)
    results["projection"] = projections(results, meta["sequencing_series"], total_experiments)
    write_atomic(out / "results.json", json.dumps(results, indent=2) + "\n")
    write_atomic(out / "results.md", render(results))
    return results


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def _soft_serial(fetched: dict[str, Any]) -> dict[str, Any]:
    """Return the stage 1 SOFT fetch rate: one new connection per file, held in memory.

    Its bytes are decompressed text; a 429 or 503 the client retried past is not seen.
    """
    step = fetched["steps"].get(raw.GEO_SOFT, {"seconds": 0.0, "bytes": 0, "calls": 0})
    return {
        "files": step["calls"],
        "seconds": step["seconds"],
        "text_bytes": step["bytes"],
        "files_per_s": round(step["calls"] / step["seconds"], 2) if step["seconds"] else None,
        "errors": step.get("errors", {}),
    }


def _sra_serial(fetched: dict[str, Any], pilot: Path) -> dict[str, Any]:
    """Return the stage 1 fetch's SRA ``efetch`` cost per 100 packages and per package."""
    step = fetched["steps"].get(raw.SRA_XML, {"seconds": 0.0, "bytes": 0, "calls": 0})
    folder = pilot / "raw" / raw.SRA_XML
    packages = sum(pq.ParquetFile(p).metadata.num_rows for p in folder.glob("*.parquet"))
    return {
        "calls": step["calls"],
        "packages": packages,
        "s_per_100_packages": round(step["seconds"] / packages * 100, 3) if packages else None,
        "bytes_per_package": round(step["bytes"] / packages) if packages else None,
    }


def _commit() -> str | None:
    from labdata.store.provenance import produced_by

    return produced_by("scripts/store/pilot_stage2.py").get("commit")


def stability(r: dict[str, Any]) -> dict[str, Any]:
    """Judge a step stable: no 429 or 503 and timeouts under 1% of requests, per source."""
    soft, sra = r["soft_bench"], r["sra_bench"]
    soft_events = soft.get("throttle_events", {}) if soft else {}
    sra_events = sra.get("retried", {}) if sra else {}
    throttled = {
        k: v
        for k, v in (soft_events | {f"sra {k}": v for k, v in sra_events.items()}).items()
        if "429" in k or "503" in k
    }
    soft_timeouts = soft_events.get("timeout", 0) / max(1, soft.get("files", 0)) if soft else 0
    sra_timeouts = (
        sum(v for k, v in sra_events.items() if "imeout" in k) / max(1, sra.get("calls", 0))
        if sra
        else 0
    )
    return {
        "throttled": throttled,
        "soft_timeout_share": round(soft_timeouts, 4),
        "sra_timeout_share": round(sra_timeouts, 4),
        "stable": not throttled and soft_timeouts < 0.01 and sra_timeouts < 0.01,
    }


def projections(r: dict[str, Any], total_series: int, total_experiments: int) -> dict[str, Any]:
    """Return the hours to fetch every sequencing Series and every experiment at this rate."""
    soft, sra = r["soft_bench"], r["sra_bench"]
    soft_rate = soft.get("files_per_s") if soft else None
    sra_rate = sra.get("packages_per_s") if sra else None
    return {
        "total_series": total_series,
        "total_experiments": total_experiments,
        "soft_hours": round(total_series / soft_rate / 3600, 2) if soft_rate else None,
        "sra_hours": round(total_experiments / sra_rate / 3600, 2) if sra_rate else None,
        "stage1_serial_hours": (
            round(r["fetch"]["wall_s"] * total_series / r["fetch"]["series_logged"] / 3600, 2)
            if r["fetch"]["series_logged"]
            else None
        ),
    }


def render(r: dict[str, Any]) -> str:
    """Return a step's results as a short Markdown page; ``results.json`` holds everything."""
    t, f, p, s = r["targets"], r["fetch"], r["projection"], r["stability"]
    soft, sra = r["soft_bench"], r["sra_bench"]
    scale = 1 / t["fraction"]
    lines = [
        f"# Stage 2 pilot: {r['step']}",
        "",
        f"Written by `scripts/store/pilot_stage2.py` at commit `{r['commit']}`.",
        f"Targets: {t['drawn']} of {t['sequencing_series']} sequencing Series "
        f"({t['drawn_mixed']} mixed) in the {t['snapshot_date']} `geo-series` snapshot, "
        f"seed {t['seed']}. Held: {r['held_target_series']}. Gaps: {r['gaps']}.",
        f"Stable: {s['stable']} (429/503: {s['throttled'] or 'none'}; timeouts SOFT "
        f"{s['soft_timeout_share']:.2%}, SRA {s['sra_timeout_share']:.2%}).",
        "",
        "## Speed and projection",
        "",
        "| Source | Speed | Files/s | Bytes/s | Errors | Whole set, h (inferred) |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    if soft:
        errors = soft["throttle_events"] | {"404": len(soft["missing_404"])}
        errors |= {"failed": len(soft["failed"])}
        lines.append(
            f"| family SOFT, pooled | {soft['connections']} connections | "
            f"{soft['files_per_s']} | {soft['bytes_per_s']} | {errors} | {p['soft_hours']} |"
        )
    if sra:
        errors = sra["retried"] | sra["failed_calls"] | {"missing": sra["missing"]}
        lines.append(
            f"| SRA packages | {sra['inflight']} in flight, batch {sra['batch']} | "
            f"{sra['packages_per_s']} | {sra['bytes_per_s']} | {errors} | {p['sra_hours']} |"
        )
    lines += [
        "",
        f"The serial stage 1 fetch took {f['wall_s']:.0f} s for {f['series_logged']} Series; "
        f"the whole set at that rate takes {p['stage1_serial_hours']} h (inferred).",
        "",
        "| Stage 1 step | Calls | Seconds | Bytes | Errors |",
        "| --- | --- | --- | --- | --- |",
    ]
    lines += [
        f"| {k} | {v['calls']} | {v['seconds']:.0f} | {v['bytes']} | {v['errors'] or ''} |"
        for k, v in f["steps"].items()
    ]
    lines += [
        "",
        f"Retried by the client during the stage 1 fetch: {f['retried'] or 'none'}.",
        f"Failed Series: {f['failed_series']}.",
        "",
    ]
    lines += [f"- {k}: {n}" for k, n in f["failures_by_kind"].items()]
    lines += [
        "",
        f"Skipped by the SOFT type re-filter: {f['soft_type_skipped']}.",
        "",
        "## Bytes per source",
        "",
        "| Source | Rows | Text bytes | Parquet bytes | Parquet, whole set (inferred) "
        "| Row p50 / p99 / max |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for k, v in r["bytes"].items():
        q = v["row_text_bytes"]
        spread = f"{q.get('p50', 0)} / {q.get('p99', 0)} / {q.get('max', 0)}"
        lines.append(
            f"| {k} | {v['rows']} | {v['text_bytes']} | {v['parquet_bytes']} | "
            f"{v['parquet_bytes'] * scale:.0f} | {spread} |"
        )
    g = r["bytes"][raw.GEO_SOFT]
    if g["rows"]:
        lines += [
            "",
            f"SOFT text {g['text_bytes']} B; with sample and platform tables stripped "
            f"{g['stripped_text_bytes']} B. Per file: `soft-sizes.tsv`, gzip sizes "
            "`soft-gz-sizes.tsv`.",
        ]
    lines += ["", "## Builds", "", "| Build | Wall s | Peak RSS bytes | Bytes |"]
    lines += ["| --- | --- | --- | --- |"]
    lines += [
        f"| {b['build_id']} | {b['wall_s']:.0f} | {b['maxrss_bytes']} | {b['bytes']} |"
        for b in r["builds"]
    ]
    return "\n".join(lines) + "\n"


def run_step(
    pilot: Path,
    step: str,
    client: EntrezClient,
    *,
    fraction: float,
    soft_connections: int,
    sra_inflight: int,
    batch: int = 300,
    seed: int = DEFAULT_SEED,
    chunk: int = DEFAULT_CHUNK,
    stage1_fetch: bool = True,
    total_experiments: int = DEFAULT_EXPERIMENTS,
    soft_base: str = "https://ftp.ncbi.nlm.nih.gov",
) -> dict[str, Any]:
    """Run one step: draw, pooled SOFT bench, stage 1 fetch, SRA bench, then the report.

    Each part is skipped when its result file exists, so a killed step resumes.
    """
    draw_step(pilot, step, fraction, seed, chunk)
    soft_bench(pilot, step, connections=soft_connections, base=soft_base)
    if stage1_fetch:
        run_fetch(pilot, step, client)
    sra_bench(pilot, step, client, inflight=sra_inflight, batch=batch)
    return report(pilot, step, total_experiments)


# ---------- command line ----------


def main(argv: list[str] | None = None) -> int:
    """Run one pilot command."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("setup")
    p.add_argument("--main", type=Path, required=True, help="the main store root, read only")
    p.add_argument("--pilot", type=Path, required=True)
    p = sub.add_parser("step")
    p.add_argument("--pilot", type=Path, required=True)
    p.add_argument("--name", required=True, help="the step's folder under <pilot>/pilot/")
    p.add_argument("--fraction", type=float, required=True)
    p.add_argument("--soft-connections", type=int, required=True)
    p.add_argument("--sra-inflight", type=int, required=True)
    p.add_argument("--batch", type=int, default=300, help="SRA experiments per efetch")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p.add_argument("--chunk", type=int, default=DEFAULT_CHUNK, help="Series per fetch call")
    p.add_argument(
        "--no-stage1-fetch", action="store_true", help="re-time only; raw/ is already fetched"
    )
    p.add_argument("--total-experiments", type=int, default=DEFAULT_EXPERIMENTS)
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
    elif args.command == "step":
        run_step(
            args.pilot,
            args.name,
            EntrezClient(allow_prompt=False),
            fraction=args.fraction,
            soft_connections=args.soft_connections,
            sra_inflight=args.sra_inflight,
            batch=args.batch,
            seed=args.seed,
            chunk=args.chunk,
            stage1_fetch=not args.no_stage1_fetch,
            total_experiments=args.total_experiments,
        )
        print(pilot_dir(args.pilot, args.name) / "results.md")
    elif args.command == "build":
        print(json.dumps(run_build(args.pilot, args.build_id)))
    else:
        report(args.pilot, args.name, args.total_experiments)
        print(pilot_dir(args.pilot, args.name) / "results.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
