"""Tests for labdata.geo.Experiment."""

import pytest

from labdata.exceptions import AccessionError, EntrezError
from labdata.geo import Experiment, Run
from tests import _geodata as g


@pytest.mark.parametrize("bad", ["SRR9000001", "SRX", "", "GSM1"])
def test_invalid_accession_raises(bad: str) -> None:
    with pytest.raises(AccessionError):
        Experiment(bad)


@pytest.mark.parametrize("good", ["SRX1", "ERX123", "DRX999"])
def test_valid_accession_prefixes(good: str) -> None:
    assert Experiment(good).accession == good


def test_accession_is_normalized() -> None:
    assert Experiment("  srx1  ").accession == "SRX1"


def test_metadata() -> None:
    e = Experiment(g.SRX, client=g.build_client())
    assert e.uid == g.SRA_UID
    assert e.title == "scRNA-seq of LUNG_N01"
    assert e.study == "SRP200000"
    assert e.organism == "Homo sapiens"
    assert e.instrument_model == "Illumina HiSeq 2500"


def test_runs_are_instances() -> None:
    runs = Experiment(g.SRX, client=g.build_client()).runs
    assert runs == [Run(g.SRR1), Run(g.SRR2)]
    assert all(isinstance(r, Run) for r in runs)


def test_runs_are_seeded_from_experiment_summary() -> None:
    client = g.build_client()
    e = Experiment(g.SRX, client=client)
    runs = e.runs
    before = len(client.calls)
    # A run shares its experiment's sra record, so its stats need no extra request.
    assert runs[0].total_spots == 600
    assert runs[0].total_bases == 1200
    assert runs[1].total_spots == 400
    assert runs[0].experiment == e
    assert len(client.calls) == before


def test_url() -> None:
    assert Experiment(g.SRX).url == "https://www.ncbi.nlm.nih.gov/sra/?term=SRX5921017"


def test_no_record_raises() -> None:
    e = Experiment(g.SRX, client=g.build_client().but(esearch={}))
    with pytest.raises(EntrezError, match="no SRA Experiment"):
        _ = e.uid
