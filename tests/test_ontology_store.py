"""The ontology store's skeleton: init, finding the root, and lookup over a tiny store."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import NotAStoreError, OntologyError, StoreNotConfiguredError
from labdata.ontology import (
    Reason,
    RetiredEntry,
    Scope,
    SnapshotData,
    SourceTerm,
    Synonym,
    Term,
    build_tiny_store,
    open_ontology_store,
)
from labdata.ontology.tables import write_snapshot

UBERON = SnapshotData(
    "uberon",
    "2026-11-05",
    [
        SourceTerm(
            "UBERON:0002421",
            "hippocampal formation",
            synonyms=(Synonym("hippocampus", Scope.NARROW, "INCONSISTENT"),),
        )
    ],
)
CL = SnapshotData(
    "cl",
    "2026-10-01",
    [
        SourceTerm("CL:0000000", "cell"),
        SourceTerm(
            "CL:0000001", "primary cultured cell", obsolete=True, replaced_by=("CLO:0000001",)
        ),
        SourceTerm("CL:0000002", "obsolete cell line cell", obsolete=True),
    ],
)
CHEBI = SnapshotData(
    "chebi", "257", [SourceTerm("CHEBI:18357", "noradrenaline", alt_ids=("CHEBI:1",))]
)
# A real EFO shape: a kit term with two lines of ancestry, one ending at an imported OBI term.
EFO = SnapshotData(
    "efo",
    "3.96.0",
    [
        SourceTerm("EFO:0009922", "10x 3' v3", parents=("EFO:0030003", "EFO:0010183")),
        SourceTerm("EFO:0030003", "10x 3' transcription profiling", parents=("EFO:0030080",)),
        SourceTerm("EFO:0030080", "10x transcription profiling"),
        SourceTerm("EFO:0010183", "single cell library construction", parents=("OBI:0000711",)),
        SourceTerm("EFO:0000001", "a", parents=("EFO:0000002",), alt_ids=("EFO:0000009",)),
        SourceTerm("EFO:0000002", "b", parents=("EFO:0000001",)),
        SourceTerm("EFO:0000003", "obsolete", obsolete=True, parents=("EFO:0030080",)),
    ],
)
TAXONOMY = SnapshotData(
    "ncbitaxon",
    "2026-11-21",
    [SourceTerm("NCBITaxon:9606", "Homo sapiens")],
    retired=[
        RetiredEntry("NCBITaxon:9605", "NCBITaxon:9606", Reason.MERGED),
        RetiredEntry("NCBITaxon:1", None, Reason.DELETED),
    ],
    prefixes=("NCBITaxon:",),
)


@pytest.fixture
def tiny(tmp_path: Path) -> Path:
    return build_tiny_store(tmp_path / "tiny", [UBERON, CL, CHEBI, EFO, TAXONOMY])


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_ONTOLOGIES", raising=False)


def test_init_makes_a_store_found_by_argument_env_and_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "ontologies"
    result = CliRunner().invoke(app, ["ontology", "init", str(root)])
    assert result.exit_code == 0, result.output

    with open_ontology_store() as store:  # the pointer init saved
        assert store.root == root
        assert store.snapshots == {}
    with open_ontology_store(root) as store:
        assert store.root == root
    other = tmp_path / "other"
    assert CliRunner().invoke(app, ["ontology", "init", str(other)]).exit_code == 0
    monkeypatch.setenv("LABDATA_ONTOLOGIES", str(root))
    with open_ontology_store() as store:  # the variable wins over the pointer, now at other
        assert store.root == root

    again = CliRunner().invoke(app, ["ontology", "init", str(root)])
    assert again.exit_code == 1
    assert "already" in again.output


def test_opening_no_store_says_how_to_point_to_one(tmp_path: Path) -> None:
    with pytest.raises(StoreNotConfiguredError, match="LABDATA_ONTOLOGIES"):
        open_ontology_store()
    with pytest.raises(NotAStoreError, match=r"labdata-ontologies\.toml") as raised:
        open_ontology_store(tmp_path)
    for way in ("--root", "LABDATA_ONTOLOGIES", "labdata ontology init"):
        assert way in str(raised.value)


def test_a_live_term_looks_up_with_its_name_and_snapshot(tiny: Path) -> None:
    with open_ontology_store(tiny) as store:
        assert store.snapshots == {
            "chebi": "chebi-257",
            "cl": "cl-2026-10-01",
            "efo": "efo-3.96.0",
            "ncbitaxon": "ncbitaxon-2026-11-21",
            "uberon": "uberon-2026-11-05",
        }
        assert store.lookup(" UBERON:0002421 ") == Term(
            "UBERON:0002421", "hippocampal formation", False, (), None, "uberon-2026-11-05"
        )


def test_an_obsolete_term_looks_up_with_its_replacement_in_another_vocabulary(
    tiny: Path,
) -> None:
    with open_ontology_store(tiny) as store:
        assert store.lookup("CL:0000001") == Term(
            "CL:0000001",
            "primary cultured cell",
            True,
            ("CLO:0000001",),
            Reason.OBSOLETE,
            "cl-2026-10-01",
        )
        gone = store.lookup("CL:0000002")
    assert gone is not None
    assert (gone.obsolete, gone.replaced_by, gone.retired) == (True, (), Reason.OBSOLETE)


def test_alternative_merged_and_deleted_ids_resolve_as_retired(tiny: Path) -> None:
    with open_ontology_store(tiny) as store:
        assert store.lookup("CHEBI:1") == Term(
            "CHEBI:1", None, False, ("CHEBI:18357",), Reason.ALTERNATIVE, "chebi-257"
        )
        merged = store.lookup("NCBITaxon:9605")
        deleted = store.lookup("NCBITaxon:1")
    assert merged is not None
    assert (merged.replaced_by, merged.retired) == (("NCBITaxon:9606",), Reason.MERGED)
    assert deleted is not None
    assert (deleted.replaced_by, deleted.retired) == ((), Reason.DELETED)


def test_an_id_the_vocabulary_never_had_returns_none(tiny: Path) -> None:
    with open_ontology_store(tiny) as store:
        assert store.lookup("UBERON:9999999") is None
        assert store.lookup("CLO:0000001") is None  # a replacement no snapshot holds
        assert store.lookup("hippocampus") is None


def test_ancestors_walk_every_is_a_line_and_stop_at_another_vocabulary(tiny: Path) -> None:
    with open_ontology_store(tiny) as store:
        assert store.ancestors("EFO:0009922") == {
            "EFO:0030003",
            "EFO:0030080",
            "EFO:0010183",
            "OBI:0000711",
        }
        assert store.is_under("EFO:0009922", "EFO:0030080")
        assert store.is_under("EFO:0009922", "OBI:0000711")
        assert store.is_under("EFO:0009922", "EFO:0009922")
        assert not store.is_under("EFO:0030080", "EFO:0009922")


def test_an_is_a_cycle_ends_and_an_alternative_id_answers_for_its_term(tiny: Path) -> None:
    with open_ontology_store(tiny) as store:
        assert store.ancestors("EFO:0000001") == {"EFO:0000001", "EFO:0000002"}
        assert store.ancestors("EFO:0000009") == {"EFO:0000001", "EFO:0000002"}
        assert store.is_under("EFO:0000002", "EFO:0000009")


def test_an_obsolete_unknown_or_unowned_id_is_under_nothing(tiny: Path) -> None:
    with open_ontology_store(tiny) as store:
        for id in ("EFO:0000003", "EFO:9999999", "OBI:0000711", 9606):
            assert store.ancestors(id) == frozenset()
            assert not store.is_under(id, "EFO:0030080")
        assert not store.is_under("EFO:9999999", "EFO:9999999")


def test_an_open_store_keeps_its_pinned_snapshots_after_a_switch(tiny: Path) -> None:
    newer = SnapshotData("uberon", "2026-12-03", [SourceTerm("UBERON:0002421", "hippocampus")])
    with open_ontology_store(tiny) as store:
        build_tiny_store(tiny, [newer])
        shutil.rmtree(tiny / "snapshots" / "uberon-2026-11-05")  # as a prune would
        term = store.lookup("UBERON:0002421")
        assert term is not None
        assert (term.name, term.snapshot) == ("hippocampal formation", "uberon-2026-11-05")
        assert store.snapshots["uberon"] == "uberon-2026-11-05"
    with open_ontology_store(tiny) as store:
        term = store.lookup("UBERON:0002421")
    assert term is not None
    assert (term.name, term.snapshot) == ("hippocampus", "uberon-2026-12-03")


def test_lookup_command_prints_the_term(tiny: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LABDATA_ONTOLOGIES", str(tiny))
    result = CliRunner().invoke(app, ["ontology", "lookup", "CL:0000001"])
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "id: CL:0000001",
        "name: primary cultured cell",
        "obsolete: true",
        "retired: obsolete",
        "replaced by: CLO:0000001",
        "snapshot: cl-2026-10-01",
    ]
    unknown = CliRunner().invoke(app, ["ontology", "lookup", "CL:9999999"])
    assert unknown.exit_code == 1
    assert "no current snapshot has this id" in unknown.output


def test_the_writer_moves_sources_in_and_refuses_a_bad_scope(tiny: Path) -> None:
    source = tiny / "work" / "downloads" / "cl.obo"
    source.parent.mkdir(parents=True)
    source.write_text("format-version: 1.2\n", encoding="utf-8")

    assert write_snapshot(tiny, CL, sources=[source]) == "cl-2026-10-01"

    assert (tiny / "snapshots" / "cl-2026-10-01" / "source" / "cl.obo").is_file()
    wide = Synonym("y", "WIDE")  # pyright: ignore[reportArgumentType] - an untyped caller's
    bad = SnapshotData("pato", "2026-10-01", [SourceTerm("PATO:1", "x", synonyms=(wide,))])
    with pytest.raises(OntologyError, match="WIDE"):
        write_snapshot(tiny, bad)
    assert not (tiny / "snapshots" / "pato-2026-10-01").exists()
