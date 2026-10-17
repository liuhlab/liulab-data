"""The E-utilities lane: the experiment list, and SRA packages fetched in parts at the seam."""

from __future__ import annotations

import gzip
import io
import json
import logging
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

import labdata.ncbi.entrez as entrez_mod
from labdata.ncbi._http import RateLimiter
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.store import build, eutils, gaps, pack, raw

TODAY = "2026-10-15"
_HEADER = (
    "Accession\tSubmission\tStatus\tUpdated\tPublished\tReceived\tType\tCenter\tVisibility\t"
    "Alias\tExperiment\tSample\tStudy\tLoaded\tSpots\tBases\tMd5sum\tBioSample\tBioProject\t"
    "ReplacedBy"
)


def _tab_line(accession: str, status: str, kind: str, center: str, alias: str) -> str:
    cells = [accession, "SRA1", status, "-", "-", "-", kind, center, "public", alias]
    return "\t".join(cells + ["-"] * 10)


def _soft(*experiments: str) -> str:
    lines = ["^SERIES = GSE1"]
    for i, srx in enumerate(experiments):
        lines += [
            f"^SAMPLE = GSM{i}",
            f"!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term={srx}",
        ]
    return "\n".join(lines) + "\n"


def _store(tmp_path: Path, tab: list[str], soft: dict[str, str] | None = None) -> Path:
    root = tmp_path / "store"
    folder = root / "snapshots" / "sra-accessions" / "2026-10-01"
    folder.mkdir(parents=True)
    (folder / "SRA_Accessions.tab").write_text("\n".join([_HEADER, *tab]) + "\n")
    if soft:
        rows = [raw.RawRow(gse, text, TODAY) for gse, text in soft.items()]
        pack.write_rows(root, raw.GEO_SOFT, rows)
    return root


def _geo(*pairs: tuple[str, str]) -> list[str]:
    return [_tab_line(acc, status, "EXPERIMENT", "GEO", f"GSM{acc[3:]}") for acc, status in pairs]


def _list(root: Path) -> list[tuple[str, str]]:
    table = pq.read_table(eutils.experiment_list(root))
    return list(zip(table["accession"].to_pylist(), table["status"].to_pylist(), strict=True))


# ---------------------------------------------------------------- the experiment list


def test_the_experiment_list_is_the_union_of_soft_relations_and_geo_tab_rows(
    tmp_path: Path,
) -> None:
    tab = [
        _tab_line("SRX1", "live", "EXPERIMENT", "GEO", "GSM11"),
        _tab_line("SRX2", "live", "EXPERIMENT", "GEO", "not-a-sample"),
        _tab_line("SRX3", "live", "EXPERIMENT", "BGI", "x"),
        _tab_line("SRX5", "suppressed", "EXPERIMENT", "GEO", "GSM55"),
        _tab_line("SRR6", "live", "RUN", "GEO", "GSM66"),
        _tab_line("SRX1000001", "live", "EXPERIMENT", "GEO", "GSM77"),
    ]
    root = _store(tmp_path, tab, {"GSE1": _soft("SRX1", "SRX3"), "GSE2": _soft("SRX9", "SRX1")})
    # a SOFT part the GEO lane wrote and nothing has packed yet counts too
    piece = raw.Piece(raw.GEO_SOFT, raw.bucket_of(raw.GEO_SOFT, "GSE3"), ("GSE3",))
    raw.write_part(root, piece, [raw.RawRow("GSE3", _soft("SRX20"), TODAY)])

    assert _list(root) == [
        ("SRX1", "live"),
        ("SRX3", "live"),  # named only by SOFT: its status still comes from the tab
        ("SRX5", "suppressed"),
        ("SRX9", ""),  # in no tab row
        ("SRX20", ""),
        ("SRX1000001", "live"),
    ]


# ---------------------------------------------------------------- the fetch, at the seam


def _package(srx: str) -> str:
    return f'<EXPERIMENT_PACKAGE><EXPERIMENT accession="{srx}"/></EXPERIMENT_PACKAGE>'


@dataclass
class _Reply:
    body: bytes
    status: int = 200
    _stream: io.BytesIO | None = None

    def getheader(self, name: str, default: str | None = None) -> str | None:
        return default

    def read(self, amt: int | None = None) -> bytes:
        if self._stream is None:
            self._stream = io.BytesIO(self.body)
        return self._stream.read(amt)


class _Killed(BaseException):
    """Stands in for a job killed mid-fetch."""


@dataclass
class _Ncbi:
    """Answers ``efetch db=sra`` with a package per known experiment, as NCBI does."""

    known: set[str]
    left_out: set[str] = field(default_factory=set)
    broken: set[str] = field(default_factory=set)
    broken_calls: int = 0
    delay: float = 0.0
    kill_after: int | None = None
    slow: set[str] = field(default_factory=set)
    on_slow: Callable[[], None] = lambda: None
    asked: list[list[str]] = field(default_factory=list)
    starts: list[float] = field(default_factory=list)
    peak: int = 0
    _open: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @contextmanager
    def open(self, method, url, *, body, headers, timeout) -> Iterator[_Reply]:
        ids = urllib.parse.parse_qs(body.decode())["id"][0].split(",")
        with self._lock:
            if self.kill_after is not None and len(self.asked) >= self.kill_after:
                raise _Killed
            self.asked.append(ids)
            self.starts.append(time.monotonic())
            self._open += 1
            self.peak = max(self.peak, self._open)
        try:
            time.sleep(self.delay)
            if set(ids) & self.slow:
                self.on_slow()
            yield self._answer(ids)
        finally:
            with self._lock:
                self._open -= 1

    def _answer(self, ids: list[str]) -> _Reply:
        if set(ids) & self.broken:
            return _Reply(b"", 500)
        if self.broken_calls:
            self.broken_calls -= 1
            return _Reply(b"", 500)
        served = [i for i in ids if i in self.known and i not in self.left_out]
        self.left_out -= set(ids)  # left out once, served when asked again
        if not served and not set(ids) & self.known:
            return _Reply(b"<ERROR>ID list is empty! Possibly it has no correct IDs.</ERROR>", 400)
        return _Reply("".join(_package(i) for i in served).encode())


def _client(ncbi: _Ncbi, rate: float = 1000.0) -> EntrezClient:
    creds = NcbiCredentials("t@t.org", "abc")
    return EntrezClient(creds, transport=ncbi, limiter=RateLimiter(rate, 8))


def _run(root: Path, ncbi: _Ncbi, **kw: Any) -> eutils.LaneReport:
    return eutils.fetch_packages(
        root, eutils.experiment_list(root), client=_client(ncbi), date=TODAY, **kw
    )


def _packed(root: Path, source: str) -> dict[str, str]:
    pack.pack(root, [source])
    return {row.accession: row.raw_text for row in raw.read_rows(root, source)}


@pytest.fixture
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)


def test_a_short_batch_saves_the_rest_and_records_the_missing_once(
    tmp_path: Path, no_backoff: None
) -> None:
    root = _store(tmp_path, _geo(("SRX1", "live"), ("SRX2", "live"), ("SRX5", "suppressed")))
    ncbi = _Ncbi(known={"SRX1", "SRX2"}, left_out={"SRX2"})

    report = _run(root, ncbi)

    assert (report.fetched, report.missing, report.failed) == (2, 1, 0)
    assert ncbi.asked == [["SRX1", "SRX2", "SRX5"], ["SRX2", "SRX5"]]
    rerun = _Ncbi(known={"SRX1", "SRX2", "SRX5"})
    assert _run(root, rerun).fetched == 0
    assert rerun.asked == []  # the missing experiment is not refetched
    packed = _packed(root, raw.SRA_XML)
    assert packed == {"SRX1": _package("SRX1"), "SRX2": _package("SRX2"), "SRX5": ""}


def test_a_batch_that_fails_is_fetched_again_on_the_next_run(
    tmp_path: Path, no_backoff: None
) -> None:
    root = _store(tmp_path, _geo(("SRX1", "live"), ("SRX7", "live")))

    report = _run(root, _Ncbi(known={"SRX1", "SRX7"}, broken={"SRX7"}), batch=1)

    assert (report.fetched, report.failed) == (1, 1)
    again = _Ncbi(known={"SRX1", "SRX7"})
    assert _run(root, again).fetched == 1
    assert again.asked == [["SRX7"]]


def test_a_batch_that_fails_is_asked_again_before_the_lane_ends(
    tmp_path: Path, no_backoff: None
) -> None:
    root = _store(tmp_path, _geo(("SRX1", "live")))
    ncbi = _Ncbi(known={"SRX1"}, broken_calls=entrez_mod._MAX_TRIES)

    report = _run(root, ncbi)

    assert (report.fetched, report.failed) == (1, 0)
    assert _packed(root, raw.SRA_XML) == {"SRX1": _package("SRX1")}


def test_eight_calls_stay_in_flight_under_the_rate_cap(tmp_path: Path) -> None:
    accessions = [(f"SRX{i}", "live") for i in range(1, 49)]
    root = _store(tmp_path, _geo(*accessions))
    ncbi = _Ncbi(known={a for a, _ in accessions}, delay=0.05)
    rate = 200.0

    eutils.fetch_packages(
        root, eutils.experiment_list(root), client=_client(ncbi, rate), date=TODAY, batch=2
    )

    assert len(ncbi.asked) == 24
    assert ncbi.peak == 8
    starts = sorted(ncbi.starts)
    window = 0.05
    assert max(sum(s <= t < s + window for t in starts) for s in starts) <= rate * window + 3


def test_a_resumed_lane_refetches_no_saved_part_and_packs_the_same_raw(
    tmp_path: Path,
) -> None:
    accessions = [(f"SRX{i}", "live") for i in range(1, 21)]
    known = {a for a, _ in accessions}
    whole = _store(tmp_path / "whole", _geo(*accessions))
    _run(whole, _Ncbi(known=known), batch=2, part_size=4, inflight=1)
    assert len(raw.part_files(whole, raw.SRA_XML)) == 5  # one bucket, split into parts

    cut = _store(tmp_path / "cut", _geo(*accessions))
    with pytest.raises(_Killed):
        _run(cut, _Ncbi(known=known, kill_after=5), batch=2, part_size=4, inflight=1)
    saved = {a for p in raw.part_files(cut, raw.SRA_XML) for a in raw.asked(p)}
    assert len(saved) == 8  # two whole parts; the third was cut
    resumed = _Ncbi(known=known)
    _run(cut, resumed, batch=2, part_size=4, inflight=1)

    assert not saved & {a for ids in resumed.asked for a in ids}
    assert _packed(cut, raw.SRA_XML) == _packed(whole, raw.SRA_XML)


def test_the_lane_logs_progress_with_a_projected_finish(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    root = _store(tmp_path, _geo(*[(f"SRX{i}", "live") for i in range(1, 7)]))
    caplog.set_level(logging.INFO, logger="labdata.store.eutils")

    _run(root, _Ncbi(known={"SRX1"}, delay=0.05), batch=1, log_every=0.02)

    lines = [r.getMessage() for r in caplog.records if "experiments" in r.getMessage()]
    assert len(lines) >= 3
    assert "6/6 experiments" in lines[-1]
    assert "missing 5" in lines[-1]
    assert "finish" in lines[0]


def test_a_later_round_over_more_series_asks_only_for_the_new_experiments(
    tmp_path: Path,
) -> None:
    accessions = [(f"SRX{i}", "live") for i in range(1, 9)]
    known = {a for a, _ in accessions}
    samples = {"GSE1": ["GSM1", "GSM2"], "GSE2": ["GSM3", "GSM4", "GSM5"], "GSE3": ["GSM6"]}
    soft = {"GSE1": _soft("SRX1", "SRX7"), "GSE3": _soft("SRX8")}  # SRX7, 8: SOFT only

    def store(name: str) -> Path:
        root = _store(tmp_path / name, _geo(*accessions), soft)
        folder = root / "snapshots" / "geo-series" / "2026-10-01"
        folder.mkdir(parents=True)
        with gzip.open(folder / "gds_gse_esummary.jsonl.gz", "wt") as fh:
            for gse, gsms in samples.items():
                record = {"accession": gse, "samples": [{"accession": g} for g in gsms]}
                fh.write(json.dumps(record) + "\n")
        return root

    def run(root: Path, ncbi: _Ncbi, series: set[str]) -> None:
        listed = eutils.experiment_list(root, series=series)
        eutils.fetch_packages(root, listed, client=_client(ncbi), date=TODAY, batch=2)

    rounds = store("rounds")
    first = _Ncbi(known=known)
    run(rounds, first, {"GSE1"})
    assert sorted(a for ids in first.asked for a in ids) == ["SRX1", "SRX2", "SRX7"]
    second = _Ncbi(known=known)
    run(rounds, second, {"GSE1", "GSE2", "GSE3"})
    assert sorted(a for ids in second.asked for a in ids) == [
        "SRX3",
        "SRX4",
        "SRX5",
        "SRX6",
        "SRX8",
    ]

    once = store("once")
    run(once, _Ncbi(known=known), {"GSE1", "GSE2", "GSE3"})
    assert _packed(rounds, raw.SRA_XML) == _packed(once, raw.SRA_XML)


def test_a_slow_call_holds_back_only_its_own_part(tmp_path: Path) -> None:
    accessions = [(f"SRX{i}", "live") for i in range(1, 9)]
    root = _store(tmp_path, _geo(*accessions))
    seen: list[int] = []

    def wait_for_the_others() -> None:
        deadline = time.monotonic() + 5
        while len(raw.part_files(root, raw.SRA_XML)) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        seen.append(len(raw.part_files(root, raw.SRA_XML)))

    ncbi = _Ncbi(known={a for a, _ in accessions}, slow={"SRX1"}, on_slow=wait_for_the_others)
    _run(root, ncbi, batch=2, part_size=2, inflight=2)

    assert seen == [3]  # the three other parts were saved while the first call hung
    assert len(raw.part_files(root, raw.SRA_XML)) == 4


# ---------------------------------------------------------------- bad saved packages


def test_a_saved_empty_package_of_a_live_experiment_is_asked_for_again(
    tmp_path: Path, no_backoff: None
) -> None:
    root = _store(tmp_path, _geo(("SRX1", "live"), ("SRX3", "live")))
    _run(root, _Ncbi(known={"SRX1"}))
    assert _packed(root, raw.SRA_XML) == {"SRX1": _package("SRX1"), "SRX3": ""}
    ncbi = _Ncbi(known={"SRX1", "SRX3"})

    report = _run(root, ncbi)

    assert ncbi.asked == [["SRX3"]]
    assert (report.reasked, report.recovered, report.failed) == (1, 1, 0)
    assert _packed(root, raw.SRA_XML) == {"SRX1": _package("SRX1"), "SRX3": _package("SRX3")}


_SPLICED = (Path(__file__).parent / "data" / "sra_package" / "SRX10340496-spliced.xml").read_text()


def _saved_spliced(tmp_path: Path) -> Path:
    """A store holding SRX10340496 as spliced, which its current build notes as unparsable."""
    root = _store(tmp_path, _geo(("SRX10340496", "live")))
    pack.write_rows(root, raw.SRA_XML, [raw.RawRow("SRX10340496", _SPLICED, TODAY)])
    folder = root / "builds" / "b1"
    folder.mkdir(parents=True)
    note = gaps.Note("SRX10340496", gaps.UNPARSABLE_PACKAGE, "not well-formed XML as saved")
    gaps.write_notes(root, folder, folder / "no-target-list.parquet", [note])
    build.write_marker(root, {"format_version": 1, "build": "b1"})
    return root


def test_a_saved_spliced_package_is_asked_for_again_and_replaced(
    tmp_path: Path, no_backoff: None
) -> None:
    root = _saved_spliced(tmp_path)
    ncbi = _Ncbi(known={"SRX10340496"})

    report = _run(root, ncbi)

    assert ncbi.asked == [["SRX10340496"]]
    assert (report.reasked, report.recovered) == (1, 1)
    assert _packed(root, raw.SRA_XML) == {"SRX10340496": _package("SRX10340496")}


def test_a_spliced_package_ncbi_again_serves_none_for_stays_and_is_asked_next_run(
    tmp_path: Path, no_backoff: None
) -> None:
    root = _saved_spliced(tmp_path)
    ncbi = _Ncbi(known=set())

    report = _run(root, ncbi)

    assert ncbi.asked[0] == ["SRX10340496"]
    assert (report.reasked, report.recovered, report.failed) == (1, 0, 0)
    assert _packed(root, raw.SRA_XML) == {"SRX10340496": _SPLICED}
    later = _Ncbi(known=set())
    _run(root, later)
    assert later.asked[0] == ["SRX10340496"]


@pytest.mark.parametrize("status", ["unpublished", "suppressed"])
def test_a_saved_empty_package_of_an_experiment_not_live_is_not_asked_again(
    tmp_path: Path, no_backoff: None, status: str
) -> None:
    root = _store(tmp_path, _geo(("SRX1", "live"), ("SRX5", status)))
    _run(root, _Ncbi(known={"SRX1"}))
    ncbi = _Ncbi(known={"SRX1", "SRX5"})

    report = _run(root, ncbi)

    assert ncbi.asked == []  # nothing to ask again makes no call
    assert report == eutils.LaneReport(0, 0, 0)
