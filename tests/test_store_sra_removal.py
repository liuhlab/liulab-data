"""The yearly SRA removal: ``labdata store remove-sra`` on the thin-slice store, then pack."""

from __future__ import annotations

import gzip
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from typer.testing import CliRunner

import labdata.ncbi.entrez as entrez_mod
from labdata._http import RateLimiter
from labdata.cli import app
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.store import raw, sra_removal
from tests._slices import DATA, built_root
from tests.test_store_refresh_fetch import Ncbi, _Reply

_NEW = "2026-11-01"
_TAB_HEADER = (
    "Accession\tSubmission\tStatus\tUpdated\tPublished\tReceived\tType\tCenter\tVisibility\t"
    "Alias\tExperiment\tSample\tStudy\tLoaded\tSpots\tBases\tMd5sum\tBioSample\tBioProject\t"
    "ReplacedBy\n"
)
_GONE_GDS = "<eSummaryResult><ERROR>UID={uid}: cannot get document summary</ERROR></eSummaryResult>"
_PACKAGES = DATA / "geo_slice" / "packages"

#: Held experiments of the thin slice and what the tests do to them.
SUPPRESSED = "SRX017370"  # the tab marks it suppressed
RUN_SRX, RUN = "SRX017372", "SRR037177"  # the tab marks one run of a live experiment withdrawn
WITHDRAWN_GSE, ITS_SRX = "GSE233931", "SRX20570567"  # a Series GEO dropped, its one experiment
DROPPED_GSE, DROPPED_SRX = "GSE153830", "SRX8670456"  # another dropped Series and experiment


@dataclass
class _Sra(Ncbi):
    """NCBI serving the thin slice's packages; ``failing`` ids answer 500 when in a request."""

    failing: set[str] = field(default_factory=set)
    gds_gone: set[str] = field(default_factory=set)

    def _answer(self, url: str, params: dict[str, str]) -> _Reply:
        if url.endswith("esummary.fcgi"):
            uid = params["id"]
            if f"GSE{int(uid) - 200_000_000}" in self.gds_gone:
                return _Reply(_GONE_GDS.format(uid=uid).encode())
            return _Reply(
                f"<eSummaryResult><DocSum><Id>{uid}</Id></DocSum></eSummaryResult>".encode()
            )
        if url.endswith("efetch.fcgi") and self.failing & set(params["id"].split(",")):
            self.fetched += params["id"].split(",")
            return _Reply(b"", 500)
        return super()._answer(url, params)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)


def _ncbi() -> _Sra:
    """SRA a year on: the suppressed experiment and the dropped Series' one are gone."""
    ncbi = _Sra()
    ncbi.packages = {p.stem: p.read_text(encoding="utf-8") for p in _PACKAGES.iterdir()}
    del ncbi.packages[SUPPRESSED], ncbi.packages[DROPPED_SRX]
    ncbi.packages[RUN_SRX] = re.sub(
        rf'<RUN\b[^>]*accession="{RUN}".*?</RUN>', "", ncbi.packages[RUN_SRX], flags=re.S
    )
    return ncbi


def _store(tmp_path: Path) -> Path:
    """The thin slice built, then snapshots renewed: two Series dropped, three SRA rows not live."""
    root = built_root(tmp_path / "store", "geo_slice")
    old = next((root / "snapshots" / "geo-series").iterdir())
    folder = root / "snapshots" / "geo-series" / _NEW
    folder.mkdir(parents=True)
    with (
        gzip.open(old / "gds_gse_esummary.jsonl.gz", "rt", encoding="utf-8") as src,
        gzip.open(folder / "gds_gse_esummary.jsonl.gz", "wt", encoding="utf-8") as dst,
    ):
        for line in src:
            if json.loads(line)["accession"] not in (WITHDRAWN_GSE, DROPPED_GSE):
                dst.write(line)
    rows = [
        (SUPPRESSED, "suppressed", "EXPERIMENT", "-"),
        (RUN, "withdrawn", "RUN", RUN_SRX),
        ("SRR037171", "live", "RUN", SUPPRESSED),
        ("SRX999999", "suppressed", "EXPERIMENT", "-"),  # not held
    ]
    tab = root / "snapshots" / "sra-accessions" / _NEW / "SRA_Accessions.tab"
    tab.parent.mkdir(parents=True)
    tab.write_text(
        _TAB_HEADER
        + "".join(
            f"{a}\tSRA1\t{s}\t-\t-\t-\t{k}\tGEO\tpublic\t-\t{e}\t-\t-\t-\t-\t-\t-\t-\t-\t-\n"
            for a, s, k, e in rows
        ),
        encoding="utf-8",
    )
    return root


def _use(monkeypatch: pytest.MonkeyPatch, ncbi: Ncbi) -> None:
    def client() -> EntrezClient:
        creds = NcbiCredentials("t@t.org", "abc")
        return EntrezClient(creds, transport=ncbi, limiter=RateLimiter(1000.0, 8))

    monkeypatch.setattr("labdata.cli._store_client", client)


def _step(root: Path, *args: str) -> str:
    result = CliRunner().invoke(app, ["store", *args, "--root", str(root)])
    assert result.exit_code == 0, result.output
    return result.output


def _held(root: Path, source: str) -> dict[str, str]:
    return {row.accession: row.raw_text for row in raw.read_rows(root, source)}


def _runs(package: str) -> list[str]:
    return re.findall(r'<RUN\b[^>]*\baccession="(\w+)"', package)


def test_only_candidates_ncbi_confirms_gone_are_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _store(tmp_path)
    before = _held(root, raw.SRA_XML)
    ncbi = _ncbi()
    _use(monkeypatch, ncbi)

    said = _step(root, "remove-sra")
    _step(root, "pack")

    after = _held(root, raw.SRA_XML)
    assert set(before) - set(after) == {SUPPRESSED, DROPPED_SRX}
    # the dropped Series' experiment NCBI still serves stays
    assert after[ITS_SRX] == before[ITS_SRX]
    assert RUN in _runs(before[RUN_SRX])
    assert _runs(after[RUN_SRX]) == [r for r in _runs(before[RUN_SRX]) if r != RUN]
    unchanged = set(after) - {RUN_SRX}
    assert {a: after[a] for a in unchanged} == {a: before[a] for a in unchanged}
    # only candidates are asked: two non-public, two no live Series reaches
    assert set(ncbi.fetched) == {SUPPRESSED, RUN_SRX, ITS_SRX, DROPPED_SRX}
    assert "4 candidates" in said
    assert "2 confirmed gone, 1 runs gone, 1 still served, 0 failed and kept" in said


def test_a_fetch_error_keeps_a_candidate_and_a_rerun_asks_only_it_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    root = _store(tmp_path)
    ncbi = _ncbi()
    ncbi.failing = {SUPPRESSED}
    _use(monkeypatch, ncbi)
    monkeypatch.setattr(sra_removal, "BATCH", 2)

    with caplog.at_level("INFO"):
        said = _step(root, "remove-sra")
    _step(root, "pack")

    assert "1 confirmed gone, 1 runs gone, 1 still served, 1 failed and kept" in said
    assert "confirmed 2 of 4:" in caplog.text
    assert "confirmed 4 of 4:" in caplog.text
    assert SUPPRESSED in _held(root, raw.SRA_XML)

    ncbi.failing = set()
    ncbi.fetched.clear()
    said = _step(root, "remove-sra")
    _step(root, "pack")

    assert ncbi.fetched == [SUPPRESSED]
    assert "1 asked" in said
    assert SUPPRESSED not in _held(root, raw.SRA_XML)


def test_a_series_removal_and_an_sra_removal_go_in_the_same_pack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _store(tmp_path)
    ncbi = _ncbi()
    ncbi.gds_gone = {WITHDRAWN_GSE}
    _use(monkeypatch, ncbi)

    assert "1 confirmed gone" in _step(root, "_confirm")
    _step(root, "remove-sra")
    _step(root, "pack")

    assert WITHDRAWN_GSE not in _held(root, raw.GEO_SOFT)
    assert DROPPED_GSE in _held(root, raw.GEO_SOFT)
    assert not {SUPPRESSED, DROPPED_SRX} & set(_held(root, raw.SRA_XML))
