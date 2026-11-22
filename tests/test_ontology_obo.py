"""The OBO reader: a file's own terms, with every field the store keeps."""

from __future__ import annotations

from pathlib import Path

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
