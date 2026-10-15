"""The GEO lane over a fake transport: real client, real gzip streams, no socket."""

from __future__ import annotations

import gzip
import hashlib
import logging
import re
import threading
import tracemalloc
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import pytest

import labdata.ncbi.entrez as entrez_mod
from labdata.ncbi._http import RateLimiter
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.store import geo_lane, pack, provenance, raw
from labdata.store.fetch import filelist_url, soft_url

DATE = "2026-10-15"

HEADERS = (
    "^DATABASE = GeoMiame\n"
    "!Database_name = Gene Expression Omnibus (GEO)\n"
    "^SERIES = {gse}\n"
    "!Series_title = A study\n"
    "!Series_type = Expression profiling by high throughput sequencing\n"
)
PLATFORM = (
    "^PLATFORM = GPL1\n"
    "!Platform_title = A platform\n"
    "!platform_table_begin\n"  # GEO writes some markers in lower case
    "ID\tSEQUENCE\n"
    "1\tACGT\n"
    "!platform_table_end\n"
)
SAMPLE = (
    "^SAMPLE = GSM1\n"
    "!Sample_title = A sample\n"
    "!Sample_data_row_count = 1\n"
    "!Sample_table_begin\n"
    "ID_REF\tVALUE\n"
    "1\t2.5\n"
    "!Sample_table_end\n"
    "!Sample_description = after the table\n"
)


def soft(gse: str, extra: str = "") -> bytes:
    return (HEADERS.format(gse=gse) + extra + PLATFORM + SAMPLE).encode()


@dataclass
class Reply:
    body: bytes = b""
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    offset: int = 0

    def getheader(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name, default)

    def read(self, amt: int | None = None) -> bytes:
        end = len(self.body) if amt is None else self.offset + amt
        data, self.offset = self.body[self.offset : end], min(end, len(self.body))
        return data


Answer = bytes | int | BaseException


class Site:
    """GEO's download site: each URL answers its canned answers in turn, the last repeating.

    An unmapped URL answers 404.
    """

    def __init__(self, pages: dict[str, list[Answer]] | None = None) -> None:
        self.pages = {url: list(answers) for url, answers in (pages or {}).items()}
        self.calls: list[str] = []
        self.open_now = 0
        self.most_open = 0
        self._lock = threading.Lock()

    @contextmanager
    def open(self, method, url, *, body, headers, timeout) -> Iterator[Reply]:
        with self._lock:
            self.calls.append(urllib.parse.urlsplit(url).path)
            answers = self.pages.get(url, [404])
            answer = answers.pop(0) if len(answers) > 1 else answers[0]
            self.open_now += 1
            self.most_open = max(self.most_open, self.open_now)
        try:
            if isinstance(answer, BaseException):
                raise answer
            if isinstance(answer, int):
                yield Reply(b"", answer)
            else:
                yield Reply(answer)
        finally:
            with self._lock:
                self.open_now -= 1


def client(site: Site) -> EntrezClient:
    creds = NcbiCredentials("t@t.org", "abc")
    return EntrezClient(creds, transport=site, limiter=RateLimiter(1000.0, 8))


@pytest.fixture(autouse=True)
def no_waits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(geo_lane, "_sleep", lambda _s: None)


def saved(root: Path, source: str) -> dict[str, str]:
    pack.pack(root)
    return {row.accession: row.raw_text for row in raw.read_rows(root, source)}


def test_soft_is_stored_without_data_tables_and_with_the_originals_size_and_sha256(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(entrez_mod, "_CHUNK_BYTES", 7)  # chunks split lines and markers
    original = soft("GSE1")
    site = Site({soft_url("GSE1"): [gzip.compress(original)]})
    geo_lane.run(tmp_path, ["GSE1"], client=client(site), date=DATE)
    assert saved(tmp_path, raw.GEO_SOFT)["GSE1"] == (
        f"#labdata_original_size = {len(original)}\n"
        f"#labdata_original_sha256 = {hashlib.sha256(original).hexdigest()}\n"
        "^DATABASE = GeoMiame\n"
        "!Database_name = Gene Expression Omnibus (GEO)\n"
        "^SERIES = GSE1\n"
        "!Series_title = A study\n"
        "!Series_type = Expression profiling by high throughput sequencing\n"
        "^PLATFORM = GPL1\n"
        "!Platform_title = A platform\n"
        "^SAMPLE = GSM1\n"
        "!Sample_title = A sample\n"
        "!Sample_data_row_count = 1\n"
        "!Sample_description = after the table\n"
    )
    readme = (provenance.document_raw(tmp_path, raw.GEO_SOFT) / "README.md").read_text()
    assert "data tables stripped" in readme
    assert "#labdata_original_sha256" in readme


def test_a_soft_404_is_a_soft_missing_gap_and_a_filelist_404_an_empty_result(
    tmp_path: Path,
) -> None:
    array = soft("GSE1").replace(b"high throughput sequencing", b"array")
    superseries = soft("GSE3", "!Series_relation = SuperSeries of: GSE4\n")
    site = Site(
        {
            soft_url("GSE1"): [gzip.compress(array)],
            soft_url("GSE3"): [gzip.compress(superseries)],
            soft_url("GSE4"): [gzip.compress(soft("GSE4"))],
            filelist_url("GSE3"): [b"Archive/File\tName\n"],
        }
    )
    report = geo_lane.run(tmp_path, ["GSE1", "GSE2", "GSE3"], client=client(site), date=DATE)
    assert report == geo_lane.LaneReport(held=3, soft_missing=1, fetch_failed=0, filelists=3)
    assert sorted(saved(tmp_path, raw.GEO_SOFT)) == ["GSE1", "GSE3", "GSE4"]
    assert saved(tmp_path, raw.GEO_GAPS) == {"GSE2": "soft-missing"}
    assert saved(tmp_path, raw.GEO_FILELIST) == {
        "GSE1": "",
        "GSE3": "Archive/File\tName\n",
        "GSE4": "",
    }


def test_a_rerun_requests_nothing_saved_and_retries_a_fetch_failed_series(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(geo_lane, "PROGRESS_SECONDS", 0.001)
    site = Site(
        {
            soft_url("GSE1"): [gzip.compress(soft("GSE1"))],
            soft_url("GSE3"): [TimeoutError("timed out")],
        }
    )
    with caplog.at_level(logging.INFO, logger="labdata.store.geo_lane"):
        report = geo_lane.run(tmp_path, ["GSE1", "GSE2", "GSE3"], client=client(site), date=DATE)
    assert (report.held, report.soft_missing, report.fetch_failed) == (1, 1, 1)
    assert site.calls.count(urllib.parse.urlsplit(soft_url("GSE3")).path) == geo_lane.TRIES
    assert "GEO site timeout: connections now 2" in caplog.text
    assert re.search(r"\d/3 Series SOFT, round 1 \(.*\), [\d.]+/s, errors: timeout \d", caplog.text)

    site.pages[soft_url("GSE3")] = [TimeoutError("timed out"), gzip.compress(soft("GSE3"))]
    site.calls.clear()
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="labdata.store.geo_lane"):
        report = geo_lane.run(tmp_path, ["GSE1", "GSE2", "GSE3"], client=client(site), date=DATE)
    assert sorted(site.calls) == sorted(
        urllib.parse.urlsplit(u).path
        for u in (soft_url("GSE3"), soft_url("GSE3"), filelist_url("GSE3"))
    )
    assert (report.held, report.soft_missing, report.fetch_failed) == (2, 1, 0)
    assert "connections now" not in caplog.text  # one timeout is a stall, not load


def test_a_later_round_requests_only_new_series_and_packs_as_one_run_would(
    tmp_path: Path,
) -> None:
    pages: dict[str, list[Answer]] = {
        soft_url(f"GSE{n}"): [gzip.compress(soft(f"GSE{n}"))] for n in (1, 2, 3, 10_001)
    }
    nested, whole = tmp_path / "nested", tmp_path / "whole"
    site = Site(pages)
    geo_lane.run(nested, ["GSE1", "GSE10001"], client=client(site), date=DATE)
    site.calls.clear()
    geo_lane.run(nested, ["GSE1", "GSE2", "GSE3", "GSE10001"], client=client(site), date=DATE)
    assert sorted(site.calls) == sorted(
        urllib.parse.urlsplit(u).path
        for n in (2, 3)
        for u in (soft_url(f"GSE{n}"), filelist_url(f"GSE{n}"))
    )
    geo_lane.run(whole, ["GSE1", "GSE2", "GSE3", "GSE10001"], client=client(Site(pages)), date=DATE)
    for source in (raw.GEO_SOFT, raw.GEO_FILELIST):
        assert saved(nested, source) == saved(whole, source)


def test_a_large_soft_streams_through_without_being_held_in_memory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(entrez_mod, "_CHUNK_BYTES", 1 << 16)
    rows = "".join(f"{i}\t{i * 7919 % 100_003}\n" for i in range(400_000))
    table = f"!Sample_table_begin\nID_REF\tVALUE\n{rows}!Sample_table_end\n"
    original = soft("GSE1").replace(
        b"!Sample_table_begin\nID_REF\tVALUE\n1\t2.5\n!Sample_table_end\n", table.encode()
    )
    body = gzip.compress(original)
    size = len(original)
    del rows, table
    site = Site({soft_url("GSE1"): [body]})
    tracemalloc.start()
    geo_lane.run(tmp_path, ["GSE1"], client=client(site), date=DATE)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak < size / 4
    text = saved(tmp_path, raw.GEO_SOFT)["GSE1"]
    assert geo_lane.original(text) == (size, hashlib.sha256(original).hexdigest())
    assert text.endswith("!Sample_description = after the table\n")
    assert len(text) < 2000
