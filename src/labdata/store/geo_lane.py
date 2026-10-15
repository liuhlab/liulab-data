"""The GEO lane: family SOFT and ``filelist.txt`` of every target Series, from GEO's site.

Each Series' family SOFT is streamed over a few kept-open HTTPS connections with its sample
and platform data tables stripped on the way; the stored text starts with the original
file's size and sha256. Every Series and SubSeries met is fetched whatever its SOFT type.
A SOFT 404 or exhausted retries is saved as a ``raw/geo-gaps/`` row; a ``fetch-failed``
Series is fetched again on the next run, a ``soft-missing`` one is not. Each held Series
then gets its ``filelist.txt``, a 404 saved as an empty text. Results are written as parts
for the pack step; a saved item makes no request.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import re
import threading
import time
from collections import Counter, deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from labdata.exceptions import EntrezError, PageNotFound
from labdata.ncbi.entrez import EntrezClient
from labdata.store import gaps, raw
from labdata.store.fetch import filelist_url, soft_url

#: Connections kept open to GEO's download site.
CONNECTIONS = 4
#: Tries per file before a Series is a ``fetch-failed`` gap.
TRIES = 4
#: Most Series one part holds, so a cut-off run loses little.
PART_SIZE = 500
#: Seconds between progress lines.
PROGRESS_SECONDS = 60.0
#: Seconds without a throttle before one connection is added back.
RECOVER_SECONDS = 300.0
#: Timeouts among the last ``TIMEOUT_WINDOW`` tries that halve the connections. GEO's site
#: stalls on the odd request even when idle; only repeated timeouts signal load.
TIMEOUT_LIMIT = 3
TIMEOUT_WINDOW = 20
#: The header lines the stored SOFT starts with.
SIZE_LINE = "#labdata_original_size = "
SHA256_LINE = "#labdata_original_sha256 = "

_TABLE_BEGIN = (b"!sample_table_begin", b"!platform_table_begin")
_TABLE_END = (b"!sample_table_end", b"!platform_table_end")
_SUBSERIES = re.compile(r"^!Series_relation = SuperSeries of: (GSE\d+)", re.M)
_sleep = time.sleep
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LaneReport:
    """What one run of the lane holds: Series with SOFT, gaps by reason, filelists."""

    held: int
    soft_missing: int
    fetch_failed: int
    filelists: int


def run(
    root: Path,
    series: Iterable[str],
    *,
    client: EntrezClient,
    date: str | None = None,
    connections: int = CONNECTIONS,
) -> LaneReport:
    """Fetch the family SOFT of each Series and its SubSeries, then each held one's filelist.

    Parameters
    ----------
    root : pathlib.Path
        The store root; parts go under ``work/parts/``.
    series : iterable of str
        The target Series.
    client : EntrezClient
        The NCBI seam; only its ``stream`` is used.
    date : str or None
        The fetch date to record; defaults to today's UTC date.
    connections : int
        Connections kept open; halved on HTTP 429, 503 or repeated timeouts.

    Returns
    -------
    LaneReport
        Counts over every Series met, saved before this run or now.
    """
    date = date or dt.datetime.now(dt.UTC).date().isoformat()
    todo = sorted({s.strip().upper() for s in series})
    seen: set[str] = set()
    with _Pool(client, connections) as pool:
        rounds = 0
        while todo:
            rounds += 1
            seen |= set(todo)
            _fetch_soft(root, pool, todo, date, f"soft round {rounds}")
            todo = sorted(_named_subseries(root, set(todo)) - seen)
        held = sorted(_soft_held(root, seen))
        _fetch_filelists(root, pool, held, date)
    reasons = Counter(text.split("\n", 1)[0] for text in _gap_texts(root, seen - set(held)))
    return LaneReport(
        held=len(held),
        soft_missing=reasons[gaps.SOFT_MISSING],
        fetch_failed=reasons[gaps.FETCH_FAILED],
        filelists=len(_accessions(root, raw.GEO_FILELIST, set(held))),
    )


def original(text: str) -> tuple[int, str] | None:
    """Return the size and sha256 of the file a stored SOFT text was stripped from.

    Returns
    -------
    tuple of (int, str) or None
        The original's bytes and hex sha256, or ``None`` for a text stored unstripped.
    """
    head = text.split("\n", 2)
    if len(head) < 3 or not head[0].startswith(SIZE_LINE) or not head[1].startswith(SHA256_LINE):
        return None
    return int(head[0].removeprefix(SIZE_LINE)), head[1].removeprefix(SHA256_LINE)


def strip_soft(lines: Iterable[bytes]) -> str:
    """Return family SOFT without its sample and platform data tables, size and sha256 first.

    Each table goes with its begin and end lines; every other line is kept as served.
    """
    digest = hashlib.sha256()
    size = 0
    kept: list[bytes] = []
    inside = False
    for line in lines:
        digest.update(line)
        size += len(line)
        head = line[:24].lower()
        if head.startswith(_TABLE_BEGIN):
            inside = True
        if not inside:
            kept.append(line)
        elif head.startswith(_TABLE_END):
            inside = False
    body = b"".join(kept).decode("utf-8", errors="replace")
    return f"{SIZE_LINE}{size}\n{SHA256_LINE}{digest.hexdigest()}\n{body}"


def _text(lines: Iterable[bytes]) -> str:
    return b"".join(lines).decode("utf-8", errors="replace")


def _fetch_soft(root: Path, pool: _Pool, accessions: list[str], date: str, phase: str) -> None:
    missing = {
        gse
        for gse, text in raw.saved(root, raw.GEO_GAPS, accessions).items()
        if text.split("\n", 1)[0] == gaps.SOFT_MISSING
    }
    pieces = raw.pieces(root, raw.GEO_SOFT, [a for a in accessions if a not in missing])
    with pool.phase(phase, sum(len(p.todo) for p in pieces)):
        for piece in pieces:
            for start in range(0, len(piece.todo), PART_SIZE):
                todo = piece.todo[start : start + PART_SIZE]
                found: list[raw.RawRow] = []

                def rows(todo: tuple[str, ...] = todo, found: list[raw.RawRow] = found) -> Any:
                    for gse, got in pool.map(todo, soft_url, strip_soft):
                        if isinstance(got, str):
                            yield raw.RawRow(gse, got, date)
                        else:
                            found.append(gaps.gap_row(gse, got.reason, date, got.detail))

                raw.write_part(
                    root, raw.Piece(raw.GEO_SOFT, piece.bucket, todo), rows(), record_asked=False
                )
                if found:
                    bucket = raw.bucket_of(raw.GEO_GAPS, found[0].accession)
                    gap_piece = raw.Piece(
                        raw.GEO_GAPS, bucket, tuple(sorted(r.accession for r in found))
                    )
                    raw.write_part(root, gap_piece, found, record_asked=False)


def _fetch_filelists(root: Path, pool: _Pool, held: list[str], date: str) -> None:
    pieces = raw.pieces(root, raw.GEO_FILELIST, held)
    with pool.phase("filelist", sum(len(p.todo) for p in pieces)):
        for piece in pieces:
            for start in range(0, len(piece.todo), PART_SIZE):
                todo = piece.todo[start : start + PART_SIZE]

                def rows(todo: tuple[str, ...] = todo) -> Iterator[raw.RawRow]:
                    for gse, got in pool.map(todo, filelist_url, _text):
                        if isinstance(got, str):
                            yield raw.RawRow(gse, got, date)
                        elif got.reason == gaps.SOFT_MISSING:
                            yield raw.RawRow(gse, "", date)
                        else:
                            logger.warning("filelist of %s not saved: %s", gse, got.detail)

                raw.write_part(
                    root,
                    raw.Piece(raw.GEO_FILELIST, piece.bucket, todo),
                    rows(),
                    record_asked=False,
                )


def _bucket_files(root: Path, source: str, accessions: set[str]) -> list[Path]:
    buckets = {raw.bucket_of(source, a) for a in accessions}
    paths = [root / "raw" / source / f"{b}.parquet" for b in sorted(buckets)]
    paths = [p for p in paths if p.is_file()]
    return paths + [p for p in raw.part_files(root, source) if raw.part_bucket(p) in buckets]


def _accessions(root: Path, source: str, accessions: set[str]) -> set[str]:
    """Return which of ``accessions`` have a row in ``source``, raw or part."""
    found: set[str] = set()
    for path in _bucket_files(root, source, accessions):
        column = pq.read_table(path, columns=["accession"]).column(0).to_pylist()
        found |= accessions.intersection(column)
    return found


def _soft_held(root: Path, accessions: set[str]) -> set[str]:
    return _accessions(root, raw.GEO_SOFT, accessions)


def _named_subseries(root: Path, accessions: set[str]) -> set[str]:
    """Return the SubSeries the saved SOFT of ``accessions`` name, read batch by batch."""
    named: set[str] = set()
    for path in _bucket_files(root, raw.GEO_SOFT, accessions):
        for batch in pq.ParquetFile(path).iter_batches(
            batch_size=64, columns=["accession", "raw_text"]
        ):
            for gse, text in zip(*batch.to_pydict().values(), strict=True):
                if gse in accessions:
                    named.update(_SUBSERIES.findall(text))
    return named


def _gap_texts(root: Path, accessions: set[str]) -> list[str]:
    return list(raw.saved(root, raw.GEO_GAPS, accessions).values()) if accessions else []


@dataclass(frozen=True, slots=True)
class _Gap:
    """A file not got: ``soft-missing`` for a 404, else ``fetch-failed`` with the last error."""

    reason: str
    detail: str = ""


class _Gate:
    """Lets at most ``limit`` requests run; halves on a throttle, steps back up when quiet."""

    def __init__(self, cap: int) -> None:
        self.cap = self.limit = cap
        self._open = 0
        self._timeouts: deque[bool] = deque(maxlen=TIMEOUT_WINDOW)
        self._quiet_since = time.monotonic()
        self._cond = threading.Condition()

    def __enter__(self) -> None:
        with self._cond:
            while True:
                now = time.monotonic()
                if self.limit < self.cap and now - self._quiet_since >= RECOVER_SECONDS:
                    self.limit += 1
                    self._quiet_since = now
                    logger.info("GEO site quiet: connections now %d", self.limit)
                if self._open < self.limit:
                    break
                self._cond.wait(timeout=1.0)
            self._open += 1

    def __exit__(self, *_exc: object) -> None:
        with self._cond:
            self._open -= 1
            self._cond.notify_all()

    def record(self, kind: str | None) -> None:
        """Note one try's failure kind, or ``None``; halve on a 429, 503 or repeated timeouts."""
        with self._cond:
            self._timeouts.append(kind == "timeout")
            throttled = kind in ("HTTP 429", "HTTP 503")
            if kind == "timeout" and sum(self._timeouts) >= TIMEOUT_LIMIT:
                throttled = True
                self._timeouts.clear()
            if throttled:
                self.limit = max(1, self.limit // 2)
                self._quiet_since = time.monotonic()
                logger.warning("GEO site %s: connections now %d", kind, self.limit)


class _Pool:
    """Worker threads, one kept-open connection each, behind a :class:`_Gate`."""

    def __init__(self, client: EntrezClient, connections: int) -> None:
        self._client = client
        self._gate = _Gate(connections)
        self._workers = ThreadPoolExecutor(connections, thread_name_prefix="geo-lane")
        self._progress: _Progress | None = None

    def __enter__(self) -> _Pool:
        return self

    def __exit__(self, *_exc: object) -> None:
        self._workers.shutdown(wait=True, cancel_futures=True)

    def phase(self, name: str, total: int) -> _Progress:
        self._progress = _Progress(name, total, lambda: self._gate.limit)
        return self._progress

    def map(
        self,
        items: Iterable[str],
        url: Callable[[str], str],
        consume: Callable[[Iterator[bytes]], str],
    ) -> Iterator[tuple[str, str | _Gap]]:
        """Fetch each item's URL; yield ``(item, text or gap)`` as each finishes."""
        futures = {self._workers.submit(self._one, url(i), consume): i for i in items}
        for future in as_completed(futures):
            yield futures[future], future.result()

    def _one(self, url: str, consume: Callable[[Iterator[bytes]], str]) -> str | _Gap:
        progress = self._progress
        assert progress is not None
        attempt = 0
        while True:
            attempt += 1
            kind = None
            with self._gate:
                try:
                    got: str | _Gap = self._client.stream(url, consume, tries=1)
                except PageNotFound:
                    got = _Gap(gaps.SOFT_MISSING)
                except EntrezError as err:
                    cause = err.__cause__ or err
                    kind = _kind(cause)
                    progress.error(kind)
                    got = _Gap(gaps.FETCH_FAILED, f"{kind}: {cause}")
            self._gate.record(kind)
            if not (isinstance(got, _Gap) and got.reason == gaps.FETCH_FAILED) or attempt == TRIES:
                progress.tick()
                return got
            _sleep(min(60.0, 2.0**attempt))


def _kind(err: BaseException) -> str:
    code = getattr(err, "code", None)
    if isinstance(code, int):
        return f"HTTP {code}"
    if isinstance(err, TimeoutError):
        return "timeout"
    return type(err).__name__


class _Progress:
    """Logs items done, rate, errors by kind and a projected finish, every interval."""

    def __init__(self, phase: str, total: int, connections: Callable[[], int]) -> None:
        self.phase, self.total = phase, total
        self._connections = connections
        self._done = 0
        self._errors: Counter[str] = Counter()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._start = time.monotonic()
        self._thread = threading.Thread(target=self._beat, daemon=True)

    def __enter__(self) -> _Progress:
        logger.info("%s: %d to fetch", self.phase, self.total)
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join()
        self.log()

    def tick(self) -> None:
        with self._lock:
            self._done += 1

    def error(self, kind: str) -> None:
        with self._lock:
            self._errors[kind] += 1

    def _beat(self) -> None:
        while not self._stop.wait(PROGRESS_SECONDS):
            self.log()

    def log(self) -> None:
        """Log one progress line."""
        with self._lock:
            done, errors = self._done, dict(sorted(self._errors.items()))
        wall = time.monotonic() - self._start
        rate = done / wall if wall > 0 else 0.0
        left = self.total - done
        if left == 0:
            finish = "done"
        elif rate > 0:
            eta = dt.timedelta(seconds=round(left / rate))
            at = dt.datetime.now(dt.UTC) + eta
            finish = f"finish {at:%Y-%m-%d %H:%M} UTC (in {eta})"
        else:
            finish = "finish unknown"
        logger.info(
            "%s: %d/%d done, %.2f/s, errors %s, connections %d, %s",
            self.phase,
            done,
            self.total,
            rate,
            errors or "none",
            self._connections(),
            finish,
        )
