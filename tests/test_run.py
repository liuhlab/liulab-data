"""Tests for labdata.geo.Run."""

import pytest

from labdata.exceptions import AccessionError
from labdata.geo import Experiment, Run
from tests import _geodata as g


@pytest.mark.parametrize("bad", ["SRX5921017", "SRR", "srr1", "", "GSM1"])
def test_invalid_accession_raises(bad: str) -> None:
    with pytest.raises(AccessionError):
        Run(bad)


def test_run_stats() -> None:
    r = Run(g.SRR1, client=g.build_client())
    assert r.total_spots == 600
    assert r.total_bases == 1200
    assert r.is_public is True


def test_second_run_stats() -> None:
    r = Run(g.SRR2, client=g.build_client())
    assert r.total_spots == 400
    assert r.total_bases == 800


def test_experiment_link_is_instance() -> None:
    r = Run(g.SRR1, client=g.build_client())
    assert r.experiment == Experiment(g.SRX)
    assert isinstance(r.experiment, Experiment)


def test_url() -> None:
    assert Run(g.SRR1).url == "https://www.ncbi.nlm.nih.gov/sra/?term=SRR9000001"
