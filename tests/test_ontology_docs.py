"""The ontology store's docs page lists every declared vocabulary in its table."""

from __future__ import annotations

import re
from pathlib import Path

from labdata.ontology.vocabularies import VOCABULARIES

PAGE = Path(__file__).parents[1] / "docs" / "how-to" / "ontology-run.md"

#: A table row whose first cell is a code span: the vocabulary's name.
_ROW = re.compile(r"^\| `([^`]+)` \|", re.MULTILINE)


def test_the_docs_table_lists_the_declared_vocabularies_in_order() -> None:
    listed = _ROW.findall(PAGE.read_text(encoding="utf-8"))
    assert listed == [v.name for v in VOCABULARIES]
