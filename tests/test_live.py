"""Live integration tests against real NCBI E-utilities and GEO FTP-over-HTTP.

Marked ``network`` and deselected by default (see ``addopts`` in pyproject). Run
explicitly with ``pixi run test -m network``; they need network access and NCBI
credentials (``NCBI_EMAIL`` env var or a configured cache file).
"""

import os

import pytest

from labdata.geo import BioProject, Platform, Run, Sample, Series

pytestmark = pytest.mark.network

_needs_creds = pytest.mark.skipif(not os.environ.get("NCBI_EMAIL"), reason="NCBI_EMAIL not set")

# A fully public GEO Series with a publication, samples, platform, public SRA,
# a BioProject, and supplementary files — exercises every component.
_GSE = "GSE229022"


@_needs_creds
def test_live_series() -> None:
    s = Series(_GSE)
    assert s.uid
    assert s.title
    assert s.pubmed_id is not None
    assert s.samples
    assert all(isinstance(x, Sample) for x in s.samples)
    assert s.platforms
    assert all(isinstance(x, Platform) for x in s.platforms)
    assert all(x.accession.startswith("GPL") for x in s.platforms)
    assert s.bioprojects
    assert all(isinstance(b, BioProject) for b in s.bioprojects)
    assert all(b.accession.startswith("PRJ") for b in s.bioprojects)
    assert s.supplementary_files  # this series has public supplementary files


@_needs_creds
def test_live_supplementary_file_urls() -> None:
    s = Series(_GSE)
    urls = s.supplementary_file_urls
    assert urls
    assert all(url.startswith(s.supplementary_http_url) for url in urls)


@_needs_creds
def test_live_bioproject() -> None:
    bp = Series(_GSE).bioprojects[0]
    assert bp.uid
    assert bp.title
    assert bp.organism
    assert all(isinstance(x, Series) for x in bp.series)
    assert any(x.accession == _GSE for x in bp.series)


@_needs_creds
def test_live_experiment_and_runs() -> None:
    e = Series(_GSE).experiments[0]
    assert e.title
    assert e.instrument_model
    assert e.runs
    assert all(isinstance(r, Run) for r in e.runs)
    run = e.runs[0]
    assert run.total_spots is not None
    assert run.total_spots > 0
    assert run.experiment == e


@_needs_creds
def test_live_make_sra_run_table() -> None:
    table = Series(_GSE).make_sra_run_table()
    assert not table.empty
    assert table["Run"].str.match(r"^[SED]RR\d+$").all()
    for column in (
        "Run",
        "BioSample",
        "AvgSpotLen",
        "Bases",
        "Experiment",
        "Instrument",
        "LibraryName",
    ):
        assert column in table.columns
