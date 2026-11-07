"""A refresh's fetch: the re-fetch list, then the lanes fetching it again over a fake NCBI."""

from __future__ import annotations

import gzip
import json
import threading
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from typer.testing import CliRunner

import labdata.ncbi.entrez as entrez_mod
from labdata.cli import app
from labdata.ncbi._http import RateLimiter
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.store import changes, gaps, geo_lane, init_store, pack, raw
from labdata.store.fetch import filelist_url, soft_url

OLD = "2026-09-01"
SEQ = "Expression profiling by high throughput sequencing"
_ESEARCH = (
    '<?xml version="1.0" ?>\n<!DOCTYPE eSearchResult PUBLIC "-//NLM//DTD esearch 20060628//EN" '
    '"https://eutils.ncbi.nlm.nih.gov/eutils/dtd/20060628/esearch.dtd">\n'
    "<eSearchResult><Count>{n}</Count><RetMax>{m}</RetMax><RetStart>{s}</RetStart>"
    "<IdList>{ids}</IdList><TranslationSet/><QueryTranslation>x</QueryTranslation>"
    "</eSearchResult>"
)


def series_soft(gse: str, *lines: str, version: str = "1") -> str:
    """Family SOFT of one Series: extra Series lines, then one Sample per ``SRA`` relation."""
    head = [f"^SERIES = {gse}", f"!Series_title = version {version}", f"!Series_type = {SEQ}"]
    head += [line for line in lines if not line.startswith("SRX")]
    samples = []
    for i, srx in enumerate(line for line in lines if line.startswith("SRX")):
        samples += [
            f"^SAMPLE = GSM{gse[3:]}{i}",
            f"!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term={srx}",
        ]
    return "\n".join(head + samples) + "\n"


def package(srx: str, version: str = "1") -> str:
    return f'<EXPERIMENT_PACKAGE><EXPERIMENT accession="{srx}" v="{version}"/></EXPERIMENT_PACKAGE>'


@dataclass
class _Reply:
    body: bytes
    status: int = 200
    offset: int = 0

    def getheader(self, name: str, default: str | None = None) -> str | None:
        return default

    def read(self, amt: int | None = None) -> bytes:
        end = len(self.body) if amt is None else self.offset + amt
        data, self.offset = self.body[self.offset : end], min(end, len(self.body))
        return data


@dataclass
class Ncbi:
    """NCBI as a refresh meets it: ``esearch`` on ``gds``, ``efetch`` on ``sra``, GEO's site."""

    updated: list[str] = field(default_factory=list)
    pages: dict[str, str] = field(default_factory=dict)
    packages: dict[str, str] = field(default_factory=dict)
    terms: list[str] = field(default_factory=list)
    fetched: list[str] = field(default_factory=list)
    stalled: set[str] = field(default_factory=set)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @contextmanager
    def open(self, method, url, *, body, headers, timeout) -> Iterator[_Reply]:
        params = {k: v[0] for k, v in urllib.parse.parse_qs(body.decode() if body else "").items()}
        with self._lock:
            yield self._answer(url, params)

    def _answer(self, url: str, params: dict[str, str]) -> _Reply:
        if url.endswith("esearch.fcgi"):
            self.terms.append(params["term"])
            start, most = int(params.get("retstart", 0)), int(params["retmax"])
            uids = [str(200_000_000 + int(g[3:])) for g in self.updated][start : start + most]
            ids = "".join(f"<Id>{u}</Id>" for u in uids)
            body = _ESEARCH.format(n=len(self.updated), m=len(uids), s=start, ids=ids)
            return _Reply(body.encode())
        if url.endswith("efetch.fcgi"):
            ids = params["id"].split(",")
            self.fetched += ids
            served = [self.packages[i] for i in ids if i in self.packages]
            if not served:
                return _Reply(
                    b"<ERROR>ID list is empty! Possibly it has no correct IDs.</ERROR>", 400
                )
            return _Reply("".join(served).encode())
        self.fetched.append(url)
        if url in self.stalled:
            raise TimeoutError("timed out")
        if url not in self.pages:
            return _Reply(b"", 404)
        text = self.pages[url]
        return _Reply(gzip.compress(text.encode()) if "soft" in url else text.encode())

    def serve_series(self, gse: str, text: str, filelist: str = "") -> None:
        self.pages[soft_url(gse)] = text
        self.pages[filelist_url(gse)] = filelist


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(geo_lane, "_sleep", lambda _s: None)


def _store(tmp_path: Path, targets: list[str], others: list[str] = ()) -> Path:  # type: ignore[assignment]
    """A store with a renewed ``geo-series`` snapshot listing these target Series."""
    root = tmp_path / "store"
    init_store(root)
    path = root / "snapshots" / "geo-series" / "2026-11-01" / "gds_gse_esummary.jsonl.gz"
    path.parent.mkdir(parents=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for gse in targets:
            fh.write(json.dumps({"accession": gse, "gdstype": SEQ, "bioproject": ""}) + "\n")
        for gse in others:
            fh.write(json.dumps({"accession": gse, "gdstype": "Expression profiling by array"}))
            fh.write("\n")
    return root


def _hold(root: Path, source: str, rows: dict[str, str]) -> None:
    pack.write_rows(root, source, [raw.RawRow(a, text, OLD) for a, text in rows.items()])


def _cli(ncbi: Ncbi, monkeypatch: pytest.MonkeyPatch, *args: str) -> str:
    def client() -> EntrezClient:
        creds = NcbiCredentials("t@t.org", "abc")
        return EntrezClient(creds, transport=ncbi, limiter=RateLimiter(1000.0, 8))

    monkeypatch.setattr("labdata.cli._store_client", client)
    result = CliRunner().invoke(app, ["store", *args])
    assert result.exit_code == 0, result.output
    return result.output


# ---------------------------------------------------------------- the re-fetch list


def test_the_refetch_list_is_changed_new_and_related_target_series(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _store(tmp_path, ["GSE1", "GSE2", "GSE3", "GSE4", "GSE5", "GSE6"], others=["GSE7"])
    _hold(
        root,
        raw.GEO_SOFT,
        {
            "GSE1": series_soft("GSE1", "!Series_relation = SuperSeries of: GSE4"),
            "GSE2": series_soft("GSE2"),
            "GSE3": series_soft("GSE3", "!Series_relation = Reanalysis of: GSE9"),
            "GSE4": series_soft("GSE4"),
        },
    )
    _hold(root, raw.GEO_GAPS, {"GSE5": gaps.SOFT_MISSING})
    # GSE7 changed but is no target; GSE6 is new; the search is paged
    ncbi = Ncbi(updated=["GSE1", "GSE3", "GSE7"])
    monkeypatch.setattr(changes, "PAGE", 2)
    out = tmp_path / "refetch.txt"

    said = _cli(
        ncbi,
        monkeypatch,
        "_changes",
        "--since",
        "2026-10-05",
        "--out",
        str(out),
        "--root",
        str(root),
    )

    assert out.read_text().split() == ["GSE1", "GSE3", "GSE4", "GSE6", "GSE9"]
    assert ncbi.terms == ["gse[ETYP] AND 2026/10/02:3000[UDAT]"] * 2
    assert "5 Series to fetch again" in said


# ---------------------------------------------------------------- fetching it again


def _held_store(tmp_path: Path) -> Path:
    """A store holding GSE1, GSE2 and GSE5 with their packages, as the last refresh left it."""
    root = _store(tmp_path, ["GSE1", "GSE2", "GSE5"])
    tab = root / "snapshots" / "sra-accessions" / "2026-11-01" / "SRA_Accessions.tab"
    tab.parent.mkdir(parents=True)
    tab.write_text("Accession\tStatus\tType\tCenter\tAlias\n")
    _hold(
        root,
        raw.GEO_SOFT,
        {
            "GSE1": series_soft("GSE1", "SRX1", "SRX2"),
            "GSE2": series_soft("GSE2", "SRX20"),
            "GSE5": series_soft("GSE5", "SRX5"),
        },
    )
    _hold(root, raw.GEO_FILELIST, {"GSE1": "old list\n", "GSE2": "", "GSE5": ""})
    _hold(root, raw.SRA_XML, {a: package(a) for a in ("SRX1", "SRX2", "SRX5", "SRX20")})
    (tmp_path / "targets.txt").write_text("GSE1\nGSE2\nGSE5\n")
    (tmp_path / "refetch.txt").write_text("GSE1\n")
    return root


def _changed_ncbi() -> Ncbi:
    """GEO's new GSE1: SRX2's Sample gone, SRX3's added, and a late SubSeries line."""
    ncbi = Ncbi(packages={a: package(a, "2") for a in ("SRX1", "SRX3", "SRX5")})
    gse1 = series_soft("GSE1", "!Series_relation = SubSeries of: GSE5", "SRX1", "SRX3", version="2")
    ncbi.serve_series("GSE1", gse1, "new list\n")
    ncbi.serve_series("GSE5", series_soft("GSE5", "SRX5", version="2"))
    return ncbi


def _fetch_again(
    ncbi: Ncbi, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, root: Path, lanes: str = "both"
) -> str:
    where = ["--refetch", str(tmp_path / "refetch.txt"), "--root", str(root)]
    series = ["--series", str(tmp_path / "targets.txt")]
    said = _cli(ncbi, monkeypatch, "_geo", *series, *where)
    if lanes == "both":
        said += _cli(ncbi, monkeypatch, "_eutils", *series, *where)
    return said


def _raw(root: Path, source: str) -> dict[str, str]:
    pack.pack(root)
    return {row.accession: row.raw_text for row in raw.read_rows(root, source)}


def test_a_changed_series_replaces_its_soft_filelist_related_series_and_packages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _held_store(tmp_path)
    ncbi = _changed_ncbi()

    said = _fetch_again(ncbi, monkeypatch, tmp_path, root)

    soft = _raw(root, raw.GEO_SOFT)
    assert "version 2" in soft["GSE1"]
    assert "version 2" in soft["GSE5"]  # named by a relation line only the new text has
    assert soft["GSE2"] == series_soft("GSE2", "SRX20")
    assert _raw(root, raw.GEO_FILELIST)["GSE1"] == "new list\n"
    assert _raw(root, raw.SRA_XML) == {
        "SRX1": package("SRX1", "2"),
        "SRX2": package("SRX2"),  # NCBI serves it no more: kept as saved, for now
        "SRX3": package("SRX3", "2"),
        "SRX5": package("SRX5", "2"),
        "SRX20": package("SRX20"),
    }
    assert "1 fetched again but served none: SRX2" in said


def test_a_rerun_after_a_kill_asks_only_for_what_is_not_yet_fetched_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _held_store(tmp_path)
    (tmp_path / "refetch.txt").write_text("GSE1\nGSE2\n")
    ncbi = _changed_ncbi()
    ncbi.serve_series("GSE2", series_soft("GSE2", "SRX20", version="2"))
    ncbi.packages["SRX20"] = package("SRX20", "2")
    ncbi.stalled.add(soft_url("GSE1"))  # as if the job were killed before GSE1 was saved
    _fetch_again(ncbi, monkeypatch, tmp_path, root, lanes="geo")

    ncbi.stalled.clear()
    ncbi.fetched.clear()
    _fetch_again(ncbi, monkeypatch, tmp_path, root)

    pages = [soft_url("GSE1"), filelist_url("GSE1"), soft_url("GSE5"), filelist_url("GSE5")]
    # a left-out id is asked twice
    assert sorted(ncbi.fetched) == sorted([*pages, "SRX1", "SRX2", "SRX2", "SRX20", "SRX3", "SRX5"])
    ncbi.fetched.clear()
    _fetch_again(ncbi, monkeypatch, tmp_path, root)
    assert ncbi.fetched == []
