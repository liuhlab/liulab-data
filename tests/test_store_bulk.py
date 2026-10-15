"""Bulk calls over collections, and thin-slice records 5 and 6.

Records 5 and 6 come from ``tests/data/sra_slice/``; the bulk calls run over the GEO slice.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from labdata.exceptions import NotFound, NotMirrored
from labdata.store import open_store, raw
from labdata.store.build import build
from labdata.store.fetch import fetch
from labdata.store.model import Link
from tests._fakes import ExplodingEntrezClient
from tests._slices import built_root

_WHEAT = "PRJNA436361"
_RABBIT = "PRJNA530347"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture(scope="module")
def sra_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return built_root(tmp_path_factory.mktemp("sra") / "store", "sra_slice")


@pytest.fixture(scope="module")
def geo_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return built_root(tmp_path_factory.mktemp("geo") / "store", "geo_slice")


# ---------------------------------------------------------------- records 5 and 6


def test_record_5_has_its_paper_from_the_bioproject_xml_only(sra_root: Path) -> None:
    store = open_store(sra_root)
    assert store.links([_WHEAT], "PubMed") == {
        _WHEAT: [Link("PubMed", "PMID:30115783", "BioProject XML", forward=True)]
    }
    assert [e.accession for e in store.experiments_for(_WHEAT)] == ["SRX3751851", "SRX3751852"]


def test_record_6_has_no_paper_and_the_empty_search_is_kept(sra_root: Path) -> None:
    saved = {r.accession: r.raw_text for r in raw.read_rows(sra_root, raw.PUBMED_SI)}
    assert saved[_RABBIT] == ""
    assert open_store(sra_root).follow(_RABBIT, "PubMed") == {_RABBIT: []}
    fetch(sra_root, [_RABBIT], client=ExplodingEntrezClient(), today="2026-10-15")
    build(sra_root, "again")
    store = open_store(sra_root)
    assert store.settings["build"] == "again"
    assert store.follow(_RABBIT, "PubMed") == {_RABBIT: []}
    assert {r.accession: r.raw_text for r in raw.read_rows(sra_root, raw.PUBMED_SI)} == saved


# ---------------------------------------------------------------- bulk calls

#: GEO Series of the same depth: Series, then samples, then experiments.
_SERIES = ["GSE19456", "GSE20301", "GSE28770", "GSE233931", "GSE233685", "GSE153830"]


def _calls(root: Path, accessions: list[str]) -> dict[str, int]:
    """Count the backend calls of every bulk call over one batch, on a fresh store."""
    store = open_store(root)
    store.open_many(accessions)
    store.links(accessions)
    store.follow(accessions, "sample")
    store.experiments_for_many(accessions, partial=True)
    store.supplementary_files(accessions)
    store.raw(accessions)
    store.table("GEO Series", ["!Series_title"], where=f"accession = '{accessions[0]}'")
    return dict(store.backend.calls)


def test_backend_calls_stay_the_same_as_the_batch_grows(geo_root: Path) -> None:
    assert _calls(geo_root, _SERIES[:1]) == _calls(geo_root, _SERIES)


def test_a_walk_makes_one_link_and_one_holdings_lookup_per_level(geo_root: Path) -> None:
    store = open_store(geo_root)
    walk = store.experiments_for_many(_SERIES)
    assert walk["GSE19456"]
    assert store.backend.calls == {"held_many": 1 + 2, "links_many": 2, "raw_many": 1}


def test_bulk_calls_keep_input_order_and_sort_what_they_return(geo_root: Path) -> None:
    store = open_store(geo_root)
    order = ["GSM517440", "gsm484820", "GSE19456"]
    links = store.links(order, "SRA")
    assert list(links) == ["GSM517440", "GSM484820", "GSE19456"]
    assert links["GSM517440"] == []
    assert [(k.other, k.source) for k in links["GSM484820"]] == [
        ("SRX018673", "Entrez link"),
        ("SRX018673", "SOFT"),
        ("SRX018710", "Entrez link"),
        ("SRX018710", "SOFT"),
    ]
    assert store.follow(order, "series")["GSM484820"] == ["GSE19456", "GSE19970"]


# ---------------------------------------------------------------- table


def test_table_gives_typed_columns_and_every_value_of_each_field(geo_root: Path) -> None:
    store = open_store(geo_root)
    table = store.table("GEO Series", ["!Series_title", "!Series_type"])
    assert table.column_names == [
        "accession",
        "type",
        "status",
        "submitted",
        "updated",
        "published",
        "content_hash",
        "snapshot_date",
        "!Series_title",
        "!Series_type",
    ]
    rows = {r["accession"]: r for r in table.to_pylist()}
    assert table.column("accession").to_pylist() == sorted(rows)
    assert rows["GSE19456"]["!Series_title"] == store.open("GSE19456").field("!Series_title")
    assert rows["GSE15556"]["published"] == store.open("GSE15556").published


def test_table_filters_rows_with_a_where_clause(geo_root: Path) -> None:
    table = open_store(geo_root).table(
        "SRA experiment", [], where="accession IN ('SRX018673', 'SRX018710')"
    )
    assert table.column("accession").to_pylist() == ["SRX018673", "SRX018710"]


def test_table_of_a_type_the_build_lacks_is_empty(sra_root: Path) -> None:
    table = open_store(sra_root).table("GEO Series", ["!Series_title"])
    assert table.num_rows == 0
    assert table.column_names[-1] == "!Series_title"


def test_table_names_an_unknown_type(sra_root: Path) -> None:
    with pytest.raises(ValueError, match="GEO Series"):
        open_store(sra_root).table("Series", [])


def test_table_rejects_a_where_holding_more_than_one_statement(sra_root: Path) -> None:
    with pytest.raises(ValueError, match=";"):
        open_store(sra_root).table("SRA experiment", [], where="true; DROP TABLE x")


# ---------------------------------------------------------------- the one-record path


def test_only_table_imports_pyarrow(geo_root: Path) -> None:
    code = (
        "import sys, json; from labdata.store import open_store; "
        f"s = open_store({str(geo_root)!r}); a = ['GSE19456', 'GSM484820']; "
        "s.open_many(a); s.links(a); s.follow(a, 'SRA'); s.experiments_for_many(a); "
        "s.supplementary_files(a); s.raw(a); "
        "before = [m for m in ('pandas', 'pyarrow') if m in sys.modules]; "
        "s.table('GEO Series', []); print(json.dumps([before, 'pyarrow' in sys.modules]))"
    )
    env = {**os.environ, "XDG_CACHE_HOME": str(geo_root.parent / "cache")}
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, env=env
    )
    assert json.loads(out.stdout) == [[], True]


# ---------------------------------------------------------------- supplementary files


def test_supplementary_files_of_an_accession_not_held_raise_like_open(geo_root: Path) -> None:
    store = open_store(geo_root)
    with pytest.raises(NotMirrored, match="GSE19970"):
        store.supplementary_files(["GSE15556", "GSE19970"])
    with pytest.raises(NotFound, match="2026-10-14"):
        store.supplementary_files(["GSE999999999"])
