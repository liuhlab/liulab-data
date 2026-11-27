"""The OBO reader: a file's own terms, with every field the store keeps."""

from __future__ import annotations

from pathlib import Path

from labdata.ontology import build_tiny_store, open_ontology_store
from labdata.ontology.obo import read_obo
from labdata.ontology.tables import Scope, SourceTerm, Synonym
from labdata.ontology.vocabularies import Vocabulary

# UBERON and CL write some bare xref IRIs with OBO escapes, which fastobo's xref grammar rejects.
UBERON = rb"""format-version: 1.2
data-version: uberon/releases/2026-11-05/uberon.obo
synonymtypedef: OMO:0003004 "plural form"
ontology: uberon

[Term]
id: UBERON:0000922
name: embryo
alt_id: UBERON:0000923
synonym: "embryonic organism" EXACT []
synonym: "embryos" EXACT OMO:0003004 [http://neurolex.org/wiki/Category\:Embryonic_organism]
xref: http://neurolex.org/wiki/Category\:Embryonic_organism
xref: http://www.briancoad.com/Dictionary/R-Z\,%20Abbreviations.htm

[Term]
id: UBERON:0000001
name: obsolete embryo
is_obsolete: true
replaced_by: UBERON:0000922
"""


def test_a_term_with_an_escaped_xref_iri_is_read_with_every_kept_field(tmp_path: Path) -> None:
    path = tmp_path / "uberon.obo"
    path.write_bytes(UBERON)

    data = read_obo(path, Vocabulary("uberon", ("UBERON:",)))

    assert data.release == "2026-11-05"
    assert list(data.terms) == [
        SourceTerm(
            "UBERON:0000922",
            "embryo",
            alt_ids=("UBERON:0000923",),
            synonyms=(
                Synonym("embryonic organism", Scope.EXACT),
                Synonym("embryos", Scope.EXACT, "plural form"),
            ),
        ),
        SourceTerm("UBERON:0000001", "obsolete embryo", True, ("UBERON:0000922",)),
    ]


# EFO writes a bare "!" in an imported name, which fastobo reads as a comment, and a space in a
# synonym's xref, which its grammar rejects.
EFO = b"""format-version: 1.2
data-version: http://www.ebi.ac.uk/efo/releases/v3.96.0/efo.owl

[Term]
id: AfPO:0000263
name: !Kung

[Term]
id: efo:EFO_0008913
name: single-cell RNA sequencing
synonym: "single-cell transcriptome sequencing" EXACT [Wikipedia:Single cell sequencing]
"""


def test_a_bare_bang_in_a_name_and_a_space_in_a_synonym_xref_are_read(tmp_path: Path) -> None:
    path = tmp_path / "efo.obo"
    path.write_bytes(EFO)

    efo = read_obo(path, Vocabulary("efo", ("EFO:",)))
    afpo = read_obo(path, Vocabulary("afpo", ("AfPO:",)))

    synonym = Synonym("single-cell transcriptome sequencing", Scope.EXACT)
    assert list(efo.terms) == [
        SourceTerm("EFO:0008913", "single-cell RNA sequencing", synonyms=(synonym,))
    ]
    assert list(afpo.terms) == [SourceTerm("AfPO:0000263", "!Kung")]


# An own term's parents are kept, an imported one's own is_a is not; ids read as CURIEs.
EFO_TREE = b"""format-version: 1.2
data-version: http://www.ebi.ac.uk/efo/releases/v3.96.0/efo.owl

[Term]
id: EFO:0009922
name: 10x 3' v3
is_a: efo:EFO_0030003 ! 10x 3' transcription profiling

[Term]
id: EFO:0010183
name: single cell library construction
is_a: OBI:0000711 ! library preparation

[Term]
id: OBI:0000711
name: library preparation
is_a: BFO:0000015
"""


def test_an_own_terms_is_a_parents_are_kept_with_any_prefix(tmp_path: Path) -> None:
    path = tmp_path / "efo.obo"
    path.write_bytes(EFO_TREE)

    efo = read_obo(path, Vocabulary("efo", ("EFO:",)))

    assert {t.id: t.parents for t in efo.terms} == {
        "EFO:0009922": ("EFO:0030003",),
        "EFO:0010183": ("OBI:0000711",),
    }
    with open_ontology_store(build_tiny_store(tmp_path / "store", [efo])) as store:
        assert store.is_under("EFO:0009922", "EFO:0030003")
        assert store.is_under("EFO:0010183", "OBI:0000711")
