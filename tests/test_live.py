"""Live integration tests against real NCBI E-utilities and GEO FTP-over-HTTP.

Marked ``network`` and deselected by default (see ``addopts`` in pyproject). Run
explicitly with ``pixi run test -m network``; they need network access and NCBI
credentials (``NCBI_EMAIL`` env var or a configured cache file).
"""

import os

import pytest

from labdata.geo import Experiment, Platform, Run, Sample, Series

pytestmark = pytest.mark.network

_needs_creds = pytest.mark.skipif(not os.environ.get("NCBI_EMAIL"), reason="NCBI_EMAIL not set")

# A stable GEO Series with a publication, samples, platform, and supplementary files.
_GSE = "GSE131907"
# A stable, fully public SRA experiment (with two runs).
_SRX = "SRX079566"


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
    assert all(b.startswith("PRJ") for b in s.bioproject_ids)
    assert s.supplementary_files  # this series has public supplementary files


@_needs_creds
def test_live_experiment_and_runs() -> None:
    e = Experiment(_SRX)
    assert e.title
    assert e.instrument_model
    assert e.runs
    assert all(isinstance(r, Run) for r in e.runs)
    run = e.runs[0]
    assert run.total_spots is not None
    assert run.total_spots > 0
    assert run.experiment == e
