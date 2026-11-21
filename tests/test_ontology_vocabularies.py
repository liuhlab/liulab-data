"""All 12 vocabularies through the update over a fake web: releases, readers and lookups."""

from __future__ import annotations

import hashlib
import io
import tarfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata import _lock
from labdata.cli import app
from labdata.ontology import Reason, Scope, Term, lifecycle, open_ontology_store, paths, tables
from labdata.ontology import fetch as fetch_mod
from labdata.ontology.root import make_store
from labdata.ontology.update import update
from labdata.ontology.vocabularies import VOCABULARIES
from tests._ontology_fakes import OntologyWeb

DATA = Path(__file__).parent / "data" / "ontology"
TAXDUMP = "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz"
TAXON_DATE = "Sat, 21 Nov 2026 16:29:20 GMT"
TAXON = "ncbitaxon-2026-11-21"
CELLOSAURUS = "https://ftp.expasy.org/databases/cellosaurus/cellosaurus.obo"
CELLOSAURUS_DELETED = "https://ftp.expasy.org/databases/cellosaurus/cellosaurus_deleted_ACs.txt"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_ONTOLOGIES", raising=False)
    monkeypatch.delenv(_lock.RUN_OWNER, raising=False)
    monkeypatch.setattr(fetch_mod, "_sleep", lambda _s: None)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return make_store(tmp_path / "ontologies").parent


def _declared(name: str) -> tuple:
    return tuple(v for v in VOCABULARIES if v.name == name)


def _taxdump() -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        for member in ("names.dmp", "merged.dmp", "delnodes.dmp"):
            tar.add(DATA / "taxdump" / member, arcname=member)
    return out.getvalue()


def _serve_taxonomy(web: OntologyWeb, *, md5: str | None = None) -> bytes:
    body = _taxdump()
    digest = md5 or hashlib.md5(body).hexdigest()
    web.files[TAXDUMP] = body
    web.files[f"{TAXDUMP}.md5"] = f"{digest}  taxdump.tar.gz\n".encode()
    web.modified[TAXDUMP] = TAXON_DATE
    return body


#: Each OBO Foundry vocabulary's data-version as measured, and the release it names.
OBO_VERSIONS = {
    "uberon": ("UBERON:", "releases/2026-11-05", "2026-11-05"),
    "wbbt": ("WBbt:", "releases/2025-08-19", "2025-08-19"),
    "cl": ("CL:", "releases/2026-11-13", "2026-11-13"),
    "mondo": ("MONDO:", "releases/2026-11-03", "2026-11-03"),
    "pato": ("PATO:", "releases/2026-10-01/pato.obo", "2026-10-01"),
    "wbls": ("WBls:", "wbls/releases/2026-04-16/wbls-simple.owl", "2026-04-16"),
    "chebi": ("CHEBI:", "257", "257"),
    "fbdv": ("FBdv:", "fbdv/releases/2026-11-02", "2026-11-02"),
}


def _obo(name: str, prefix: str, version: str | None) -> bytes:
    stated = f"data-version: {version}\n" if version else "date: 05:02:2021 13:48\n"
    term = f"[Term]\nid: {prefix}0000001\nname: a {name} term\n"
    return f"format-version: 1.2\n{stated}ontology: {name}\n\n{term}".encode()


def test_all_twelve_update_and_each_snapshot_is_named_by_its_release(root: Path) -> None:
    purl = "https://purl.obolibrary.org/obo/{}"
    web = OntologyWeb()
    for name, (prefix, version, _) in OBO_VERSIONS.items():
        web.files[purl.format(f"{name}.obo")] = _obo(name, prefix, version)
    owl = (DATA / "hsapdv.owl").read_text()
    for name, prefix in (("hsapdv", "HsapDv:"), ("mmusdv", "MmusDv:")):
        web.files[purl.format(f"{name}.obo")] = _obo(name, prefix, None)
        web.files[purl.format(f"{name}.owl")] = owl.replace("hsapdv", name).encode()
    web.files[CELLOSAURUS] = (DATA / "cellosaurus.obo").read_bytes()
    web.files[CELLOSAURUS_DELETED] = (DATA / "cellosaurus_deleted_ACs.txt").read_bytes()
    _serve_taxonomy(web)

    status = update(root, transport=web)

    assert status.outcome == lifecycle.Outcome.BUILT, status
    expected = {name: f"{name}-{release}" for name, (_, _, release) in OBO_VERSIONS.items()}
    expected |= {
        "hsapdv": "hsapdv-2025-01-23",
        "mmusdv": "mmusdv-2025-01-23",
        "cellosaurus": "cellosaurus-57.0",
        "ncbitaxon": TAXON,
    }
    with open_ontology_store(root) as store:
        assert dict(store.snapshots) == expected
        assert store.lookup("HsapDv:0000001") == Term(
            "HsapDv:0000001", "a hsapdv term", False, (), None, "hsapdv-2025-01-23"
        )
    # HsapDv's release came from the OWL header alone; only its OBO file was downloaded.
    (owl_get,) = web.gets(purl.format("hsapdv.owl"))
    assert owl_get.headers["Range"] == f"bytes=0-{fetch_mod.HEAD_BYTES - 1}"


def test_taxonomy_is_named_by_its_file_date_and_looks_up_by_number(root: Path) -> None:
    web = OntologyWeb()
    _serve_taxonomy(web)

    update(root, vocabularies=_declared("ncbitaxon"), transport=web)

    with open_ontology_store(root) as store:
        assert store.snapshots == {"ncbitaxon": TAXON}
        human = Term("NCBITaxon:9606", "Homo sapiens", False, (), None, TAXON)
        assert store.lookup(9606) == human
        assert store.lookup("9606") == human
        assert store.lookup("NCBITaxon:9606") == human
        assert store.lookup("12") == Term(
            "NCBITaxon:12", None, False, ("NCBITaxon:9606",), Reason.MERGED, TAXON
        )
        assert store.lookup("3418") == Term(
            "NCBITaxon:3418", None, False, (), Reason.DELETED, TAXON
        )
    source = paths.SNAPSHOTS.at(root, TAXON, paths.SOURCE_DIR, "taxdump.tar.gz")
    assert source.is_file()


def test_a_taxdump_that_fails_its_checksum_is_not_used(root: Path) -> None:
    web = OntologyWeb()
    _serve_taxonomy(web, md5="0" * 32)

    status = update(root, vocabularies=_declared("ncbitaxon"), transport=web)

    row = status.vocabularies["ncbitaxon"]
    assert (row.result, row.snapshot) == (lifecycle.Result.FAILED, None)
    assert "checksum" in (row.reason or "")
    assert not paths.SNAPSHOTS.at(root, TAXON).exists()
    assert not any(paths.DOWNLOADS.at(root).iterdir())
    with open_ontology_store(root) as store:
        assert store.snapshots == {}


def test_searching_taxonomy_fails_with_a_message(root: Path) -> None:
    web = OntologyWeb()
    _serve_taxonomy(web)
    update(root, vocabularies=_declared("ncbitaxon"), transport=web)

    result = CliRunner().invoke(
        app, ["ontology", "search", "ncbitaxon", "Homo sapiens", "--root", str(root)]
    )

    assert result.exit_code == 1
    assert "looked up by id only" in result.output


def test_a_deleted_cellosaurus_id_looks_up_as_deleted(root: Path) -> None:
    web = OntologyWeb(
        files={
            CELLOSAURUS: (DATA / "cellosaurus.obo").read_bytes(),
            CELLOSAURUS_DELETED: (DATA / "cellosaurus_deleted_ACs.txt").read_bytes(),
        }
    )

    update(root, vocabularies=_declared("cellosaurus"), transport=web)

    snapshot = "cellosaurus-57.0"
    with open_ontology_store(root) as store:
        assert store.snapshots == {"cellosaurus": snapshot}
        assert store.lookup("CVCL_0222") == Term(
            "CVCL_0222", None, False, (), Reason.DELETED, snapshot
        )
        assert store.lookup("CVCL_B0T9") == Term(
            "CVCL_B0T9", "#132 PC3-1-SC-E8", False, (), None, snapshot
        )
        found = store.search('Henrietta Lacks cells "HL"', "cellosaurus")
    assert [(c.id, c.scope) for c in found][:1] == [("CVCL_0030", Scope.RELATED)]
    kept = paths.SNAPSHOTS.at(root, snapshot, paths.SOURCE_DIR)
    assert sorted(p.name for p in kept.iterdir()) == [
        "cellosaurus.obo",
        "cellosaurus_deleted_ACs.txt",
    ]


def test_a_raised_table_layout_remakes_every_reader_kind_from_its_kept_files(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hsapdv = "https://purl.obolibrary.org/obo/hsapdv"
    web = OntologyWeb()
    _serve_taxonomy(web)
    web.files[CELLOSAURUS] = (DATA / "cellosaurus.obo").read_bytes()
    web.files[CELLOSAURUS_DELETED] = (DATA / "cellosaurus_deleted_ACs.txt").read_bytes()
    web.files[f"{hsapdv}.obo"] = _obo("hsapdv", "HsapDv:", None)
    web.files[f"{hsapdv}.owl"] = (DATA / "hsapdv.owl").read_bytes()
    chosen = _declared("ncbitaxon") + _declared("cellosaurus") + _declared("hsapdv")
    with monkeypatch.context() as old:
        old.setattr(tables, "TABLE_LAYOUT", 1)
        update(root, vocabularies=chosen, transport=web)
    web.calls.clear()

    update(root, vocabularies=chosen, transport=web)

    assert [c.url for c in web.calls if "Range" not in c.headers] == []  # no download
    with open_ontology_store(root) as store:
        for snapshot in store.snapshots.values():
            assert tables.layout_of(root, snapshot) == tables.TABLE_LAYOUT
        assert store.lookup("12") == Term(
            "NCBITaxon:12", None, False, ("NCBITaxon:9606",), Reason.MERGED, TAXON
        )
        assert store.lookup("CVCL_0222") == Term(
            "CVCL_0222", None, False, (), Reason.DELETED, "cellosaurus-57.0"
        )
        assert store.lookup("HsapDv:0000001") == Term(
            "HsapDv:0000001", "a hsapdv term", False, (), None, "hsapdv-2025-01-23"
        )
