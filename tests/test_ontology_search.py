"""Search: ranked candidates for a label's text, from a tiny store."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import OntologyError
from labdata.ontology import (
    Candidate,
    Scope,
    SnapshotData,
    SourceTerm,
    Synonym,
    TextKind,
    build_tiny_store,
    open_ontology_store,
)

UBERON = SnapshotData(
    "uberon",
    "2026-11-05",
    [
        SourceTerm("UBERON:0000002", "hippocampus proper"),
        SourceTerm(
            "UBERON:0000005", "brain stem", synonyms=(Synonym("hippocampus", Scope.RELATED),)
        ),
        SourceTerm(
            "UBERON:0000006", "Ammon's horn", synonyms=(Synonym("hippocampus", Scope.EXACT),)
        ),
        SourceTerm("UBERON:0000004", "Hippocampus"),
        SourceTerm(
            "UBERON:0000003", "cornu ammonis", synonyms=(Synonym("hippocampus", Scope.EXACT),)
        ),
        SourceTerm("UBERON:0000001", "hippocampal formation"),
    ],
)


@pytest.fixture
def tiny(tmp_path: Path) -> Path:
    return build_tiny_store(tmp_path / "tiny", [UBERON])


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_ONTOLOGIES", raising=False)


def _search(tmp_path: Path, terms: list[SourceTerm], text: str) -> list[Candidate]:
    root = build_tiny_store(tmp_path / "one", [SnapshotData("uberon", "2026-11-05", terms)])
    with open_ontology_store(root) as store:
        return store.search(text, "uberon")


def test_tiers_rank_name_then_exact_then_other_scopes_then_shared_words(tiny: Path) -> None:
    with open_ontology_store(tiny) as store:
        found = store.search("hippocampus", "uberon")
        assert [c.id for c in found] == [
            "UBERON:0000004",  # the name
            "UBERON:0000003",  # an EXACT synonym; ties break on id
            "UBERON:0000006",
            "UBERON:0000005",  # a RELATED synonym
            "UBERON:0000002",  # a shared word
        ]
        assert [c.id for c in store.search("hippocampus", "uberon", limit=2)] == [
            "UBERON:0000004",
            "UBERON:0000003",
        ]


def test_a_longer_label_gets_shared_word_candidates_ranked_by_relevance(tmp_path: Path) -> None:
    found = _search(
        tmp_path,
        [
            SourceTerm("UBERON:0000001", "dorsal part of hippocampus"),
            SourceTerm("UBERON:0000002", "hippocampus"),
            SourceTerm("UBERON:0000003", "dorsal root ganglion"),
            SourceTerm("UBERON:0000004", "liver"),
            SourceTerm(
                "UBERON:0000005", "dHPC", synonyms=(Synonym("Dorsal hippocampus", Scope.EXACT),)
            ),
        ],
        "dorsal hippocampus",
    )
    assert [c.id for c in found[:2]] == ["UBERON:0000005", "UBERON:0000001"]
    assert {c.id for c in found[2:]} == {"UBERON:0000002", "UBERON:0000003"}


def test_a_term_matching_several_texts_appears_once_with_its_best(tmp_path: Path) -> None:
    found = _search(
        tmp_path,
        [
            SourceTerm(
                "UBERON:0000001",
                "hippocampal formation",
                synonyms=(
                    Synonym("hippocampus proper", Scope.NARROW),
                    Synonym("Hippocampus", Scope.RELATED),
                    Synonym("hippocampus", Scope.EXACT),
                ),
            ),
            SourceTerm(
                "UBERON:0000002",
                "left hippocampus region",
                synonyms=(Synonym("region of left hippocampus", Scope.EXACT),),
            ),
        ],
        "hippocampus",
    )
    assert [(c.id, c.text, c.scope) for c in found] == [
        ("UBERON:0000001", "hippocampus", Scope.EXACT),
        ("UBERON:0000002", "left hippocampus region", None),
    ]


def test_obsolete_terms_are_never_returned(tmp_path: Path) -> None:
    terms = [
        SourceTerm(
            "UBERON:0000001",
            "hippocampus",
            obsolete=True,
            synonyms=(Synonym("dorsal hippocampus", Scope.EXACT),),
        ),
        SourceTerm("UBERON:0000002", "liver"),
    ]
    assert _search(tmp_path, terms, "hippocampus") == []
    assert _search(tmp_path, terms, "dorsal") == []


def test_case_and_spaces_are_ignored_and_the_order_is_fixed(tiny: Path) -> None:
    spaced = SnapshotData(
        "mba", "2026-01-01", [SourceTerm("MBA:1", "dorsal   Hippocampus"), SourceTerm("MBA:2", "x")]
    )
    build_tiny_store(tiny, [spaced])
    with open_ontology_store(tiny) as store:
        first = store.search("hippocampus", "uberon")
        assert store.search("Hippocampus ", "uberon") == first
        assert store.search(" HIPPOCAMPUS\t", "uberon") == first
        assert store.search("hippocampus", "uberon") == first
        assert [c.id for c in store.search("Dorsal hippocampus", "mba")] == ["MBA:1"]
    with open_ontology_store(tiny) as store:
        assert store.search("hippocampus", "uberon") == first


def test_every_candidate_field_is_filled(tmp_path: Path) -> None:
    found = _search(
        tmp_path,
        [
            SourceTerm(
                "UBERON:0002421",
                "hippocampal formation",
                synonyms=(Synonym("hippocampus", Scope.NARROW, "INCONSISTENT"),),
            ),
            SourceTerm("UBERON:0001954", "Ammon's horn"),
        ],
        "hippocampus",
    )
    assert found == [
        Candidate(
            "UBERON:0002421",
            "hippocampal formation",
            "hippocampus",
            TextKind.SYNONYM,
            Scope.NARROW,
            "INCONSISTENT",
            "uberon-2026-11-05",
        )
    ]
    with open_ontology_store(tmp_path / "one") as store:
        assert store.search("ammon's horn", "uberon") == [
            Candidate(
                "UBERON:0001954",
                "Ammon's horn",
                "Ammon's horn",
                TextKind.NAME,
                None,
                None,
                "uberon-2026-11-05",
            )
        ]


def test_blank_text_finds_nothing_and_an_unknown_vocabulary_lists_those_held(
    tiny: Path,
) -> None:
    with open_ontology_store(tiny) as store:
        assert store.search("", "uberon") == []
        assert store.search(" \t ", "uberon") == []
        with pytest.raises(OntologyError, match=r"ubreon.*holds: uberon"):
            store.search("hippocampus", "ubreon")


def test_search_command_prints_the_candidates(tiny: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LABDATA_ONTOLOGIES", str(tiny))
    args = ["ontology", "search", "uberon", "hippocampus", "--limit", "2"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "snapshot: uberon-2026-11-05",
        "UBERON:0000004\tHippocampus\tname\t-\t-\tHippocampus",
        "UBERON:0000003\tcornu ammonis\tsynonym\tEXACT\t-\thippocampus",
    ]
    unknown = CliRunner().invoke(app, ["ontology", "search", "ubreon", "hippocampus"])
    assert unknown.exit_code == 1
    assert "holds: uberon" in unknown.output
