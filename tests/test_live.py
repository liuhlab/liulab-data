"""Live integration tests against real NCBI E-utilities.

Marked ``network`` and deselected by default (see ``addopts`` in pyproject). Run
explicitly with ``pixi run test -m network``; they need network access and NCBI
credentials (``NCBI_EMAIL`` env var or a configured cache file).
"""

import os

import pytest

from labdata.geo import Series

pytestmark = pytest.mark.network

# A small, stable GEO Series with an associated publication and SRA data.
_GSE = "GSE131907"


@pytest.mark.skipif(not os.environ.get("NCBI_EMAIL"), reason="NCBI_EMAIL not set")
def test_live_series_resolves() -> None:
    s = Series(_GSE)
    assert s.uid
    assert s.title
    assert s.pubmed_id is not None
    assert all(acc.startswith("GSM") for acc in s.samples)
    assert all(acc[1:].startswith("RX") for acc in s.experiments)
    assert all(acc.startswith("GPL") for acc in s.platforms)
    assert all(acc.startswith("PRJ") for acc in s.bioproject_ids)
    assert s.supplementary_file_types  # at least one file-type token
