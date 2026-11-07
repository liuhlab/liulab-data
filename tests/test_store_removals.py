"""Confirm and remove withdrawn Series: the hidden ``_confirm`` step, then pack."""

from __future__ import annotations

import gzip
import json
import shutil
import threading
import urllib.parse
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata import cli
from labdata._http import RateLimiter
from labdata.cli import app
from labdata.ncbi import entrez as entrez_mod
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.store import pack, raw, removals
from labdata.store.gaps import SOFT_MISSING, gap_row
from tests.test_store_geo_lane import Reply

_DATE = "2026-11-01"
_OLD = "2026-10-14"
_SEQ = "Expression profiling by high throughput sequencing"
_GONE_GDS = (
    b'<?xml version="1.0" ?>\n<eSummaryResult>\n'
    b"<ERROR>UID=%d: cannot get document summary</ERROR>\n</eSummaryResult>\n"
)
_EMPTY_IDS = Reply(
    b"<eFetchResult><ERROR>ID list is empty! In it there are neither IDs nor accessions."
    b"</ERROR></eFetchResult>",
    400,
)
_TAB_HEADER = (
    "Accession\tSubmission\tStatus\tUpdated\tPublished\tReceived\tType\tCenter\tVisibility\t"
    "Alias\tExperiment\tSample\tStudy\tLoaded\tSpots\tBases\tMd5sum\tBioSample\tBioProject\t"
    "ReplacedBy\n"
)
_SERIES_SOURCES = (raw.GEO_SOFT, raw.GEO_FILELIST, raw.GEO_GAPS, raw.ENTREZ_LINKS, raw.PUBMED_SI)

Answer = Reply | int | BaseException


class Ncbi:
    """E-utilities answered by ``(util, id)``; each key answers its answers in turn.

    An unmapped request answers 500.
    """

    def __init__(self, answers: Mapping[tuple[str, str], Sequence[Answer]] | None = None) -> None:
        self.answers = {key: list(a) for key, a in (answers or {}).items()}
        self.calls: list[tuple[str, str]] = []
        self._lock = threading.Lock()

    @contextmanager
    def open(self, method, url, *, body, headers, timeout) -> Iterator[Reply]:
        params = urllib.parse.parse_qs(body.decode()) if body else {}
        util = url.rsplit("/", 1)[-1].removesuffix(".fcgi")
        key = (util, params.get("id", [""])[0])
        with self._lock:
            self.calls.append(key)
            answers = self.answers.get(key, [500])
            answer = answers.pop(0) if len(answers) > 1 else answers[0]
        if isinstance(answer, BaseException):
            raise answer
        yield Reply(b"", answer) if isinstance(answer, int) else Reply(answer.body, answer.status)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)


def _use(monkeypatch: pytest.MonkeyPatch, ncbi: Ncbi) -> None:
    creds = NcbiCredentials("t@t.org", "abc")
    client = EntrezClient(creds, transport=ncbi, limiter=RateLimiter(1000.0, 8))
    monkeypatch.setattr(cli, "_store_client", lambda: client)


def _step(root: Path, *args: str) -> str:
    result = CliRunner().invoke(app, ["store", *args, "--root", str(root)])
    assert result.exit_code == 0, result.output
    return result.output


def _soft(gse: str, *lines: str) -> str:
    return "\n".join([f"^SERIES = {gse}", f"!Series_geo_accession = {gse}", *lines]) + "\n"


def _srx_line(srx: str) -> str:
    return f"!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term={srx}"


def _package(srx: str, *runs: str) -> str:
    run_set = "".join(f'<RUN accession="{r}"/>' for r in runs)
    return (
        f'<EXPERIMENT_PACKAGE><EXPERIMENT accession="{srx}"/>'
        f"<RUN_SET>{run_set}</RUN_SET></EXPERIMENT_PACKAGE>"
    )


def _served(*packages: str) -> Reply:
    body = "<EXPERIMENT_PACKAGE_SET>" + "".join(packages) + "</EXPERIMENT_PACKAGE_SET>"
    return Reply(body.encode())


def _store(root: Path, series: list[str], tab: Sequence[tuple[str, str, str, str]] = ()) -> Path:
    """A store whose renewed snapshots list these target Series and these tab rows.

    Each tab row is accession, status, type and experiment.
    """
    gds = root / "snapshots" / "geo-series" / _DATE / "gds_gse_esummary.jsonl.gz"
    gds.parent.mkdir(parents=True)
    with gzip.open(gds, "wt", encoding="utf-8") as fh:
        for gse in series:
            fh.write(json.dumps({"accession": gse, "gdstype": _SEQ, "bioproject": ""}) + "\n")
    path = root / "snapshots" / "sra-accessions" / _DATE / "SRA_Accessions.tab"
    path.parent.mkdir(parents=True)
    lines = [
        f"{acc}\tSRA1\t{status}\t-\t-\t-\t{kind}\tGEO\tpublic\t-\t{exp}\t-\t-\t-\t-\t-\t-\t-\t-\t-\n"
        for acc, status, kind, exp in tab
    ]
    path.write_text(_TAB_HEADER + "".join(lines), encoding="utf-8")
    (root / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return root


def _hold_series(root: Path, gse: str, soft: str | None = None) -> None:
    """Pack a Series' rows into every Series-keyed source."""
    pack.write_rows(root, raw.GEO_SOFT, [raw.RawRow(gse, soft or _soft(gse), _OLD)])
    pack.write_rows(root, raw.GEO_FILELIST, [raw.RawRow(gse, f"{gse}_RAW.tar", _OLD)])
    pack.write_rows(root, raw.GEO_GAPS, [gap_row(gse, SOFT_MISSING, _OLD)])
    pack.write_rows(root, raw.ENTREZ_LINKS, [raw.RawRow(gse, f"{gse}\tbioproject\t1\tP", _OLD)])
    pack.write_rows(root, raw.PUBMED_SI, [raw.RawRow(gse, "123", _OLD)])


def _held(root: Path, source: str) -> dict[str, str]:
    return {row.accession: row.raw_text for row in raw.read_rows(root, source)}


def _gds(gse: str) -> tuple[str, str]:
    return ("esummary", str(200_000_000 + int(gse[3:])))


def _live_gds(gse: str) -> Reply:
    uid = 200_000_000 + int(gse[3:])
    return Reply(f"<eSummaryResult><DocSum><Id>{uid}</Id></DocSum></eSummaryResult>".encode())


def test_a_series_confirmed_gone_leaves_raw_with_its_series_keyed_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    root = _store(tmp_path / "store", ["GSE1"])
    _hold_series(root, "GSE1")
    _hold_series(root, "GSE3")
    _use(monkeypatch, Ncbi({_gds("GSE3"): [Reply(_GONE_GDS % 200000003)]}))

    assert "1 confirmed gone" in _step(root, "_confirm")
    with caplog.at_level("INFO"):
        _step(root, "pack")

    for source in _SERIES_SOURCES:
        assert set(_held(root, source)) == {"GSE1"}, source
        assert f"{source}: removed 1 record" in caplog.text


def test_a_candidate_whose_fetch_fails_or_times_out_stays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _store(tmp_path / "store", ["GSE1"])
    for gse in ("GSE1", "GSE3", "GSE4", "GSE5", "GSE6"):
        _hold_series(root, gse)
    _use(
        monkeypatch,
        Ncbi(
            {
                _gds("GSE3"): [500],
                _gds("GSE4"): [TimeoutError("timed out")],
                _gds("GSE5"): [Reply(b"<eSummaryResult><oops")],
                _gds("GSE6"): [Reply(b"<eSummaryResult><ERROR>Database not found</ERROR>")],
            }
        ),
    )

    assert "0 confirmed gone" in _step(root, "_confirm")
    _step(root, "pack")

    assert set(_held(root, raw.GEO_SOFT)) == {"GSE1", "GSE3", "GSE4", "GSE5", "GSE6"}


def test_no_sra_record_is_a_candidate_and_each_stays_after_pack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tab = [("SRX1", "suppressed", "EXPERIMENT", "-"), ("SRR2", "suppressed", "RUN", "SRX2")]
    root = _store(tmp_path / "store", ["GSE1"], tab)
    _hold_series(root, "GSE1", _soft("GSE1", _srx_line("SRX1"), _srx_line("SRX2")))
    packages = {s: _package(s, f"SRR{s[3:]}") for s in ("SRX1", "SRX2")}
    pack.write_rows(root, raw.SRA_XML, [raw.RawRow(s, p, _OLD) for s, p in packages.items()])
    # This refresh's fetch got no package for SRX3.
    _part(root, raw.SRA_XML, raw.RawRow("SRX3", "", _DATE))
    ncbi = Ncbi({("efetch", s): [_EMPTY_IDS] for s in ("SRX1", "SRX2", "SRX3")})
    _use(monkeypatch, ncbi)

    assert "0 asked" in _step(root, "_confirm")
    _step(root, "pack")

    assert ncbi.calls == []
    assert _held(root, raw.SRA_XML) == packages | {"SRX3": ""}


def test_a_series_this_fetch_got_no_record_for_is_confirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _store(tmp_path / "store", ["GSE1", "GSE2"])
    _hold_series(root, "GSE1")
    _hold_series(root, "GSE2")
    _part(root, raw.GEO_GAPS, gap_row("GSE2", SOFT_MISSING, _DATE))
    ncbi = Ncbi({_gds("GSE2"): [Reply(_GONE_GDS % 200000002)]})
    _use(monkeypatch, ncbi)

    assert "1 confirmed gone" in _step(root, "_confirm")
    _step(root, "pack")

    assert set(_held(root, raw.GEO_SOFT)) == {"GSE1"}
    assert set(_held(root, raw.GEO_GAPS)) == {"GSE1"}
    assert ncbi.calls == [_gds("GSE2")]


def test_a_later_fetch_beats_an_older_removal_and_a_later_removal_an_older_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _store(tmp_path / "store", ["GSE1", "GSE3", "GSE5"])
    for gse in ("GSE1", "GSE3"):
        _hold_series(root, gse)
    gone = {_gds(g): [Reply(_GONE_GDS % (200_000_000 + int(g[3:])))] for g in ("GSE3", "GSE5")}
    _use(monkeypatch, Ncbi(gone))
    _part(root, raw.GEO_SOFT, raw.RawRow("GSE5", "fetched", _DATE))
    candidates = tmp_path / "candidates.txt"
    candidates.write_text("GSE3\nGSE5\n")

    _step(root, "_confirm", "--candidates", str(candidates))
    _part(root, raw.GEO_SOFT, raw.RawRow("GSE3", "came back", _DATE))
    _step(root, "pack")

    assert _held(root, raw.GEO_SOFT) == {"GSE1": _soft("GSE1"), "GSE3": "came back"}
    assert set(_held(root, raw.GEO_FILELIST)) == {"GSE1"}


def test_pack_cut_off_and_rerun_or_a_removal_applied_twice_changes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    whole = _store(tmp_path / "whole", ["GSE1"])
    cut = _store(tmp_path / "cut", ["GSE1"])
    monkeypatch.setattr(pack, "FAN_IN", 2)
    for root in (whole, cut):
        for gse in ("GSE1", "GSE3"):
            _hold_series(root, gse)
        for n in range(4):
            other = raw.RawRow(f"GSE2{n}", "kept", _DATE)
            _part(root, raw.GEO_SOFT, raw.RawRow("GSE3", f"fetch {n}", _DATE), other)
    _use(monkeypatch, Ncbi({_gds("GSE3"): [Reply(_GONE_GDS % 200000003)]}))

    _step(whole, "_confirm")
    _step(whole, "pack")
    _step(cut, "_confirm")
    _step(cut, "_confirm")
    parts = cut / "work" / "parts"
    shutil.copytree(parts, tmp_path / "kept", copy_function=shutil.copy2)
    _step(cut, "pack")
    shutil.copytree(tmp_path / "kept", parts, dirs_exist_ok=True, copy_function=shutil.copy2)
    _step(cut, "pack")

    for source in _SERIES_SOURCES:
        assert _held(cut, source) == _held(whole, source)
        assert "GSE3" not in _held(cut, source)
    assert set(_held(cut, raw.GEO_SOFT)) == {"GSE1", "GSE20", "GSE21", "GSE22", "GSE23"}
    assert not list(parts.rglob("*.parquet"))


def test_a_failed_confirmation_is_asked_again_by_the_next_refresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _store(tmp_path / "store", ["GSE1"])
    _hold_series(root, "GSE1")
    _hold_series(root, "GSE2")
    ncbi = Ncbi({_gds("GSE2"): [Reply(b"<eSummaryResult><oops"), Reply(_GONE_GDS % 200000002)]})
    _use(monkeypatch, ncbi)

    assert "1 failed and kept" in _step(root, "_confirm")
    _step(root, "pack")
    assert set(_held(root, raw.GEO_SOFT)) == {"GSE1", "GSE2"}

    assert "1 confirmed gone" in _step(root, "_confirm")
    _step(root, "pack")
    assert set(_held(root, raw.GEO_SOFT)) == {"GSE1"}


def test_confirm_logs_its_progress_per_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    root = _store(tmp_path / "store", [])
    for n in range(1, 4):
        _hold_series(root, f"GSE{n}")
    monkeypatch.setattr(removals, "BATCH", 2)
    _use(monkeypatch, Ncbi({_gds(f"GSE{n}"): [_live_gds(f"GSE{n}")] for n in range(1, 4)}))

    with caplog.at_level("INFO"):
        _step(root, "_confirm")

    assert "confirmed 2 of 3: 0 gone, 2 served, 0 failed" in caplog.text
    assert "confirmed 3 of 3: 0 gone, 3 served, 0 failed" in caplog.text


def test_writing_the_same_removal_again_keeps_its_place_before_later_rows(tmp_path: Path) -> None:
    root = _store(tmp_path / "store", ["GSE1"])
    _hold_series(root, "GSE3")
    raw.write_removals(root, raw.GEO_SOFT, ["GSE3"])
    _part(root, raw.GEO_SOFT, raw.RawRow("GSE3", "came back", _DATE))

    raw.write_removals(root, raw.GEO_SOFT, ["GSE3"])
    _step(root, "pack")

    assert _held(root, raw.GEO_SOFT) == {"GSE3": "came back"}


def _part(root: Path, source: str, *rows: raw.RawRow) -> Path:
    """Write rows as one part, as a fetch in this refresh does."""
    todo = tuple(sorted(row.accession for row in rows))
    piece = raw.Piece(source, raw.bucket_of(source, todo[0]), todo)
    return raw.write_part(root, piece, rows)
