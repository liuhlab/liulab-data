"""GEO records and links: thin-slice records 2, 3, 4, 7, 8, 9 and 11, fetched and built.

Fixtures in ``tests/data/geo_slice/`` are cut from live NCBI by ``make_fixtures.py`` beside
them: trimmed family SOFT, the packages of the experiments the kept samples name, the live
``elink`` and ``[si]`` answers, and a ``bioproject.xml`` snapshot of every BioProject named.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import AccessionError, LabdataError, NotFound, NotMirrored, StoreBuildError
from labdata.store import Store, open_store, raw
from labdata.store.build import build
from labdata.store.fetch import fetch
from tests._fakes import ExplodingEntrezClient, FakeEntrezClient

_DATA = Path(__file__).parent / "data" / "geo_slice"
_SERIES = ["GSE246310", "GSE153830", "GSE19456", "GSE233931", "GSE233685", "GSE20301", "GSE28770"]
_SUBSERIES = ["GSE246307", "GSE246308", "GSE246309"]


def _soft_url(gse: str) -> str:
    return f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse[:-3]}nnn/{gse}/soft/{gse}_family.soft.gz"


def _soft(gse: str) -> str:
    return (_DATA / "soft" / f"{gse}.soft").read_text(encoding="utf-8")


def _client() -> FakeEntrezClient:
    entrez = json.loads((_DATA / "entrez.json").read_text())
    elink: dict[tuple[str, str, str], list[str]] = {}
    for key, uids in entrez["elink"].items():
        dbfrom, db, uid = key.split()
        elink[dbfrom, db, uid] = uids
    esummary: dict[tuple[str, str], dict[str, Any]] = {}
    for uid, srx in entrez["accessions"]["sra"].items():
        esummary["sra", uid] = {"Id": uid, "ExpXml": f'<Experiment acc="{srx}" ver="1"/>'}
    for uid, prj in entrez["accessions"]["bioproject"].items():
        esummary["bioproject", uid] = {"Id": uid, "Project_Acc": prj}
    return FakeEntrezClient(
        pages={_soft_url(path.stem): _soft(path.stem) for path in (_DATA / "soft").iterdir()},
        packages={p.stem: p.read_text(encoding="utf-8") for p in (_DATA / "packages").iterdir()},
        elink=elink,
        esummary=esummary,
        esearch={f"{acc}[si]": pmids for acc, pmids in entrez["pubmed_si"].items()},
    )


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A store root holding the fixture snapshots and no build yet."""
    path = tmp_path / "store"
    shutil.copytree(_DATA / "snapshots", path / "snapshots")
    (path / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return path


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One fetch and build of every thin-slice Series, shared by the read-only checks."""
    path = tmp_path_factory.mktemp("geo") / "store"
    shutil.copytree(_DATA / "snapshots", path / "snapshots")
    (path / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    fetch(path, _SERIES, client=_client(), today="2026-10-14")
    build(path, "b1")
    return path


@pytest.fixture
def store(built: Path) -> Store:
    return open_store(built)


def _rows(path: Path) -> list[dict[str, Any]]:
    return pq.read_table(path).to_pylist()


def _sources(store: Store, accession: str, relation: str) -> dict[str, list[str]]:
    """Map each far end of one relation to the sorted sources stating it."""
    out: dict[str, list[str]] = {}
    for link in store.open(accession).links:
        if link.relation == relation:
            out.setdefault(link.other, []).append(link.source)
    return {other: sorted(sources) for other, sources in sorted(out.items())}


# ---------------------------------------------------------------- fetch


def test_fetch_packs_family_soft_per_thousand_series_folder(built: Path) -> None:
    folder = built / "raw" / "geo-soft"
    assert sorted(p.name for p in folder.glob("*.parquet")) == [
        "GSE153nnn.parquet",
        "GSE19nnn.parquet",
        "GSE20nnn.parquet",
        "GSE233nnn.parquet",
        "GSE246nnn.parquet",
        "GSE28nnn.parquet",
    ]
    rows = {r["accession"]: r for r in _rows(folder / "GSE246nnn.parquet")}
    assert sorted(rows) == [*_SUBSERIES, "GSE246310"]
    assert rows["GSE246307"]["raw_text"] == _soft("GSE246307")
    assert rows["GSE246307"]["fetch_date"] == "2026-10-14"


def test_fetch_saves_the_entrez_links_per_series_as_text(built: Path) -> None:
    rows = {
        r["accession"]: r["raw_text"] for r in _rows(built / "raw/entrez-links/GSE19nnn.parquet")
    }
    assert rows["GSE19456"].splitlines() == [
        "GSE19456\tbioproject\t120321\tPRJNA120321",
        "GSM484820\tsra\t20038\tSRX018673",
        "GSM484820\tsra\t20075\tSRX018710",
        "GSM484821\tsra\t20039\tSRX018674",
        "GSM484822\tsra\t20040\tSRX018675",
    ]


def test_fetch_pulls_packages_of_every_linked_experiment_and_si_for_each_series(
    built: Path,
) -> None:
    packages = {r.accession for r in raw.read_rows(built, raw.SRA_XML)}
    assert packages == {p.stem for p in (_DATA / "packages").iterdir()}
    searched = {r.accession: r.raw_text for r in raw.read_rows(built, raw.PUBMED_SI)}
    assert {*_SERIES, *_SUBSERIES} <= set(searched)
    assert searched["GSE20301"] == "20194744"
    assert searched["GSE19456"] == ""


def test_si_is_searched_for_every_held_bioproject_linked_or_given(store: Store) -> None:
    path = store.root / "builds" / "b1" / "holdings.parquet"
    held = {r["accession"] for r in _rows(path) if r["type"] == "BioProject"}
    searched = {r.accession for r in raw.read_rows(store.root, raw.PUBMED_SI)}
    assert "PRJNA120321" in held
    assert held <= searched


def test_a_rerun_makes_no_call_for_items_already_saved(built: Path, tmp_path: Path) -> None:
    copy = tmp_path / "again"
    shutil.copytree(built, copy)
    before = {p: p.read_bytes() for p in (copy / "raw").rglob("*.parquet")}
    report = fetch(copy, _SERIES, client=ExplodingEntrezClient(), today="2026-10-15")
    assert {p: p.read_bytes() for p in (copy / "raw").rglob("*.parquet")} == before
    assert "SRX018710" in report.experiments
    assert set(report.series) == {*_SERIES, *_SUBSERIES}


def test_a_rerun_fetches_only_what_a_killed_run_left_out(root: Path) -> None:
    fetch(root, ["GSE19456"], client=_client(), today="2026-10-14")
    lost = [r for r in raw.read_rows(root, raw.SRA_XML) if r.accession != "SRX018710"]
    (root / "raw" / "sra-xml" / f"{raw.bucket_of('SRX018710')}.parquet").unlink()
    raw.write_rows(root, raw.SRA_XML, lost)
    client = _client()
    fetch(root, ["GSE19456"], client=client, today="2026-10-15")
    assert client.calls == [("efetch", ("sra", ("SRX018710",), "full"))]


def test_entrez_calls_are_batched_across_samples(root: Path) -> None:
    client = _client()
    fetch(root, ["GSE19456", "GSE28770"], client=client, today="2026-10-14")
    assert client.count("elink") == 0
    assert client.count("elink_many") == 2
    assert client.count("esummary_many") == 2
    assert client.count("efetch") == 1


def _series_soft(gse: str, kind: str, subseries: tuple[str, ...] = ()) -> str:
    lines = [f"^SERIES = {gse}", f"!Series_geo_accession = {gse}", f"!Series_type = {kind}"]
    lines += [f"!Series_relation = SuperSeries of: {sub}" for sub in subseries]
    return "\n".join(lines) + "\n"


def test_fetch_skips_a_series_that_is_not_sequencing_and_logs_its_type(
    root: Path, caplog: pytest.LogCaptureFixture
) -> None:
    array = "Expression profiling by array"
    sequencing = "Expression profiling by high throughput sequencing"
    pages: dict[str, str | None] = {
        _soft_url("GSE900001"): _series_soft("GSE900001", array),
        _soft_url("GSE900002"): _series_soft("GSE900002", array, ("GSE900003", "GSE900004")),
        _soft_url("GSE900003"): _series_soft("GSE900003", sequencing),
        _soft_url("GSE900004"): _series_soft("GSE900004", array),
    }
    client = FakeEntrezClient(pages=pages)
    with caplog.at_level("INFO", logger="labdata.store.fetch"):
        report = fetch(root, ["GSE900001", "GSE900002"], client=client, today="2026-10-14")
    assert report.series == ("GSE900002", "GSE900003")
    assert sorted(
        raw.saved(root, raw.GEO_SOFT, ["GSE900001", "GSE900002", "GSE900003", "GSE900004"])
    ) == [
        "GSE900002",
        "GSE900003",
    ]
    assert "GSE900001" in caplog.text
    assert "GSE900004" in caplog.text
    assert array in caplog.text


def test_a_series_typed_sequencing_and_other_is_fetched(root: Path) -> None:
    report = fetch(root, ["GSE15556", "GSE233931"], client=_client(), today="2026-10-14")
    assert report.series == ("GSE15556", "GSE233931")


# ---------------------------------------------------------------- records


def test_geo_records_are_built_with_typed_columns_from_soft(store: Store) -> None:
    series = store.open("GSE19456")
    assert series.type == "GEO Series"
    assert (series.status, series.submitted, series.updated, series.published) == (
        "Public",
        "2009-12-14",
        "2019-05-15",
        "2010-12-05",
    )
    assert series.field("Series_pubmed_id") == ["20929578"]
    assert store.open("GPL9126").type == "GEO Platform"
    assert store.open("GSM484820").type == "GEO Sample"


def test_data_table_rows_are_dropped_from_fields_but_kept_in_raw_text(store: Store) -> None:
    sample = store.open("GSM484820")
    names = [name for name, _ in sample.fields]
    assert "Sample_data_row_count" in names
    assert all(not name.startswith(("sample_table", "SEQUENCE", "#")) for name in names)
    assert not any("CATGAGGATGTGCATTCCATC" in value for _, value in sample.fields)
    text = store.raw(["GSM484820"])["GSM484820"].text
    assert "CATGAGGATGTGCATTCCATC\t2\n" in text
    assert text in _soft("GSE19456")
    assert hashlib.sha256(text.encode()).hexdigest() == sample.content_hash


def test_fields_keep_repeats_in_source_order(store: Store) -> None:
    summary = store.open("GSE19456").field("Series_summary")
    assert summary[0].startswith("BACKGROUND:")
    assert summary[1] == ""


def test_every_record_hash_recomputes_from_raw(store: Store, built: Path) -> None:
    held = [r["accession"] for r in _rows(built / "builds" / "b1" / "holdings.parquet")]
    texts = store.raw(held)
    assert sorted(texts) == held
    for text in texts.values():
        assert hashlib.sha256(text.text.encode()).hexdigest() == text.content_hash


def test_a_rebuild_gives_the_same_tables(built: Path) -> None:
    build(built, "b2")
    for name in ("holdings.parquet", "links_by_from.parquet", "records/geo-sample.parquet"):
        assert _rows(built / "builds" / "b1" / name) == _rows(built / "builds" / "b2" / name)
    (built / "labdata-store.toml").write_text('format_version = 1\nbuild = "b1"\n')


# ---------------------------------------------------------------- the build checks


def test_an_unknown_relation_label_fails_the_build(root: Path) -> None:
    fetch(root, ["GSE233931"], client=_client(), today="2026-10-14")
    [row] = raw.read_rows(root, raw.GEO_SOFT)
    text = row.raw_text.replace("!Series_relation = BioProject:", "!Series_relation = Mystery:")
    raw.write_rows(root, raw.GEO_SOFT, [raw.RawRow(row.accession, text, row.fetch_date)])
    with pytest.raises(StoreBuildError, match="Mystery"):
        build(root, "bad")


def test_copies_of_a_sample_that_differ_fail_the_build(root: Path) -> None:
    fetch(root, ["GSE246310"], client=_client(), today="2026-10-14")
    rows = {r.accession: r for r in raw.read_rows(root, raw.GEO_SOFT)}
    text = rows["GSE246310"].raw_text.replace("!Sample_title = ", "!Sample_title = changed ", 1)
    raw.write_rows(root, raw.GEO_SOFT, [raw.RawRow("GSE246310", text, "2026-10-14")])
    with pytest.raises(LabdataError, match="GSE246310"):
        build(root, "bad")


# ---------------------------------------------------------------- record 2


def test_superseries_links_each_subseries_once_from_soft(store: Store) -> None:
    assert _sources(store, "GSE246310", "SuperSeries of") == {sub: ["SOFT"] for sub in _SUBSERIES}
    assert store.follow("GSE246308", "SubSeries of") == {"GSE246308": ["GSE246310"]}


def test_the_umbrella_bioproject_has_three_children(store: Store) -> None:
    assert store.follow("GSE246310", "BioProject") == {"GSE246310": ["PRJNA1032473"]}
    assert store.follow("PRJNA1032473", "umbrella of") == {
        "PRJNA1032473": ["PRJNA1032481", "PRJNA1032482", "PRJNA1032483"]
    }


def test_a_sample_in_a_subseries_and_the_superseries_takes_the_subseries_text(
    store: Store, built: Path
) -> None:
    sample = store.open("GSM8571270")
    assert sorted(sample.follow("series")) == ["GSE246307", "GSE246310"]
    text = store.raw(["GSM8571270"])["GSM8571270"].text
    assert text in _soft("GSE246307")
    assert text in _soft("GSE246310")
    assert sample.snapshot_date == "2026-10-14"


# ---------------------------------------------------------------- record 3


def test_the_series_bioproject_differs_from_its_studies_bioproject(store: Store) -> None:
    assert _sources(store, "GSE153830", "BioProject") == {"PRJNA644323": ["Entrez link", "SOFT"]}
    [experiment] = store.experiments_for("GSE153830")
    assert store.follow(experiment.study, "BioProject") == {experiment.study: ["PRJNA644324"]}


# ---------------------------------------------------------------- record 4


def test_one_sample_maps_to_two_experiments_by_soft_and_entrez(store: Store) -> None:
    assert _sources(store, "GSM484820", "SRA") == {
        "SRX018673": ["Entrez link", "SOFT"],
        "SRX018710": ["Entrez link", "SOFT"],
    }
    studies = store.follow(["SRX018673", "SRX018710"], "study")
    assert sorted(s for [s] in studies.values()) == ["SRP002215", "SRP002222"]


def test_following_series_reaches_a_series_not_held(store: Store) -> None:
    assert store.follow("GSM484820", "series") == {"GSM484820": ["GSE19456", "GSE19970"]}
    with pytest.raises(NotMirrored, match="GSE19970"):
        store.open("GSE19970")


def test_absent_and_malformed_geo_accessions(store: Store) -> None:
    with pytest.raises(NotFound, match="2026-10-14"):
        store.open("GSE1")
    with pytest.raises(AccessionError):
        store.open("GSE")


# ---------------------------------------------------------------- records 7, 8


def test_two_series_reach_one_paper_through_soft_and_no_superseries(store: Store) -> None:
    for gse in ("GSE233931", "GSE233685"):
        assert _sources(store, gse, "PubMed") == {"PMID:38114495": ["SOFT"]}
        assert store.follow(gse, "SuperSeries of") == {gse: []}
        assert store.follow(gse, "SubSeries of") == {gse: []}


# ---------------------------------------------------------------- record 9


def test_a_partial_join_series_walks_to_its_experiments(store: Store) -> None:
    assert store.follow(["GSM517436", "GSM517440"], "SRA") == {"GSM517436": [], "GSM517440": []}
    assert store.follow("GSM517437", "SRA") == {
        "GSM517437": ["SRX017370", "SRX017371", "SRX017372", "SRX017373"]
    }
    experiments = store.experiments_for("GSE20301")
    assert [e.accession for e in experiments] == [
        "SRX017370",
        "SRX017371",
        "SRX017372",
        "SRX017373",
    ]


# ---------------------------------------------------------------- record 11


def test_a_legacy_series_reaches_three_studies_and_its_umbrella_only_by_entrez(
    store: Store,
) -> None:
    experiments = store.experiments_for("GSE28770")
    studies = sorted({e.study for e in experiments})
    assert studies == ["SRP006493", "SRP006497", "SRP006504"]
    assert store.follow(studies, "BioProject") == {s: [] for s in studies}
    assert _sources(store, "GSE28770", "BioProject") == {"PRJNA63455": ["Entrez link"]}


def test_a_named_annotation_is_a_link_not_a_record(store: Store) -> None:
    assert store.follow("GSM713151", "Named Annotation") == {"GSM713151": ["NA000049219.1"]}
    with pytest.raises(LabdataError, match="not a record"):
        store.open("NA000049219.1")


# ---------------------------------------------------------------- the command


def test_the_fetch_command_takes_a_series(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("labdata.cli._store_client", _client)
    result = CliRunner().invoke(app, ["store", "fetch", "GSE19456", "--root", str(root)])
    assert result.exit_code == 0, result.output
    assert "Saved 1 GEO Series" in result.output
    assert "Saved 4 SRA packages" in result.output
