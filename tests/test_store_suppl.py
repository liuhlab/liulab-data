"""Supplementary files: thin-slice record 10, ``GSE15556``, fetched and built.

Fixtures are the Series' real family SOFT (data tables trimmed) and its real
``suppl/filelist.txt``, written by ``tests/data/geo_slice/make_fixtures.py``.
"""

from __future__ import annotations

import urllib.error
from email.message import Message
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from labdata.exceptions import AccessionError, EntrezError, PageNotFound
from labdata.store import Store, open_store, raw
from labdata.store.build import build
from labdata.store.fetch import fetch
from labdata.store.model import SupplementaryFile
from tests._fakes import FakeEntrezClient
from tests._slices import copy_snapshots

_DATA = Path(__file__).parent / "data" / "geo_slice"
_GEO = "https://ftp.ncbi.nlm.nih.gov/geo"
_SERIES = f"{_GEO}/series/GSE15nnn/GSE15556"


def _client() -> FakeEntrezClient:
    """Serve GSE15556 and GSE233931, a Series with no ``filelist.txt``."""
    return FakeEntrezClient(
        pages={
            **{
                f"{_GEO}/series/{gse[:-3]}nnn/{gse}/soft/{gse}_family.soft.gz": (
                    _DATA / "soft" / f"{gse}.soft"
                ).read_text(encoding="utf-8")
                for gse in ("GSE15556", "GSE233931")
            },
            f"{_SERIES}/suppl/filelist.txt": (_DATA / "filelist" / "GSE15556.txt").read_text(
                encoding="utf-8"
            ),
        },
        packages={p.stem: p.read_text(encoding="utf-8") for p in (_DATA / "packages").iterdir()},
    )


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def _root(path: Path) -> Path:
    copy_snapshots(_DATA / "snapshots", path)
    (path / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return path


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = _root(tmp_path_factory.mktemp("suppl") / "store")
    fetch(path, ["GSE15556", "GSE233931"], client=_client(), today="2026-10-14")
    build(path, "b1")
    return path


@pytest.fixture
def store(built: Path) -> Store:
    return open_store(built)


def test_fetch_saves_each_series_filelist_and_an_absent_one_as_empty(built: Path) -> None:
    saved = {r.accession: r.raw_text for r in raw.read_rows(built, raw.GEO_FILELIST)}
    assert saved == {
        "GSE15556": (_DATA / "filelist" / "GSE15556.txt").read_text(encoding="utf-8"),
        "GSE233931": "",
    }
    assert sorted(p.name for p in (built / "raw" / "geo-filelist").glob("*.parquet")) == [
        "GSE2nnnnn.parquet",
        "GSEnnnnn.parquet",
    ]


class _Failing(FakeEntrezClient):
    """Answers every ``filelist.txt`` with one HTTP error code, raised as ``EntrezClient`` does."""

    def __init__(self, code: int) -> None:
        super().__init__(pages=_client()._pages)
        self.code = code

    def fetch_text(self, url: str) -> str:
        if not url.endswith("filelist.txt"):
            return super().fetch_text(url)
        if self.code == 404:
            raise PageNotFound(f"no page at {url}")
        cause = urllib.error.HTTPError(url, self.code, "error", Message(), None)
        raise EntrezError(f"NCBI E-utilities request failed: {cause}") from cause


def test_a_404_saves_an_empty_filelist_and_other_failures_raise(tmp_path: Path) -> None:
    root = _root(tmp_path / "store")
    fetch(root, ["GSE15556"], client=_Failing(404), today="2026-10-14")
    assert raw.saved(root, raw.GEO_FILELIST, ["GSE15556"]) == {"GSE15556": ""}
    other = _root(tmp_path / "other")
    with pytest.raises(EntrezError, match="503"):
        fetch(other, ["GSE15556"], client=_Failing(503), today="2026-10-14")


def test_record_10_has_no_experiments_and_lists_its_read_files(store: Store) -> None:
    assert store.experiments_for("GSE15556") == []
    samples = store.follow("GSE15556", "sample")["GSE15556"]
    files = store.supplementary_files(["GSE15556", *samples])
    assert files["GSE15556"] == [
        SupplementaryFile(
            owner="GSE15556",
            name="GSE15556_RAW.tar",
            url="ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE15nnn/GSE15556/suppl/GSE15556_RAW.tar",
            size=9000960,
            date="2013-01-17T14:38:58",
            type="TAR",
            archive=None,
        )
    ]
    assert files["GSM389464"] == [
        SupplementaryFile(
            owner="GSM389464",
            name="GSM389464_fasta.FA.gz",
            url="ftp://ftp.ncbi.nlm.nih.gov/geo/samples/GSM389nnn/GSM389464/suppl/"
            "GSM389464_fasta.FA.gz",
            size=2356372,
            date="2009-04-15T17:05:57",
            type="FA",
            archive="GSE15556_RAW.tar",
        )
    ]
    reads = {f.name: f.size for fs in files.values() for f in fs if f.archive}
    assert reads == {
        "GSM389464_fasta.FA.gz": 2356372,
        "GSM389472_fasta.FA.gz": 1594594,
        "GSM389474_fasta.FA.gz": 959974,
        "GSM389476_fasta.FA.gz": 4077430,
    }
    assert files["GSM389473"] == []


def test_the_series_fields_keep_both_organisms(store: Store) -> None:
    series = store.open("GSE15556")
    # Family SOFT names a Series' organisms by taxid: 8355 X. laevis, 8364 X. tropicalis.
    assert series.field("Series_sample_taxid") == ["8355", "8364"]
    assert series.field("Series_platform_taxid") == ["8355", "8364"]


def test_one_backend_call_lists_any_batch(store: Store) -> None:
    before = store.backend.calls["supplementary_many"]
    out = store.supplementary_files(["GSM389464", "GSE233931", "GSM389472"])
    assert list(out) == ["GSM389464", "GSE233931", "GSM389472"]
    assert store.backend.calls["supplementary_many"] == before + 1
    with pytest.raises(AccessionError):
        store.supplementary_files(["GSE"])


def test_a_file_named_only_in_soft_has_no_size(built: Path) -> None:
    rows = pq.read_table(built / "builds" / "b1" / "supplementary_files.parquet").to_pylist()
    owners = [r["owner"] for r in rows]
    assert owners == sorted(owners)
    gse = [r for r in rows if r["owner"] == "GSE233931"]
    assert [r["name"] for r in gse] == [
        "GSE233931_expressed_gene_RPKM.txt.gz",
        "GSE233931_expressed_gene_reads.txt.gz",
    ]
    assert {(r["size"], r["date"], r["type"], r["archive"]) for r in gse} == {
        (None, None, None, None)
    }
