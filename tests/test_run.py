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


def test_read_structure_paired() -> None:
    r = Run(g.SRR1, client=g.build_client())
    assert r.read_lengths == [28, 94]
    assert r.n_reads == 2
    assert r.read_structure == "28+94"


def test_read_structure_single_end() -> None:
    r = Run(g.SRR2, client=g.build_client())
    assert r.read_lengths == [75]
    assert r.n_reads == 1
    assert r.read_structure == "75"


def test_read_structure_fetched_once() -> None:
    client = g.build_client()
    r = Run(g.SRR1, client=client)
    assert (r.read_structure, r.read_lengths, r.n_reads) == ("28+94", [28, 94], 2)
    # The full XML is fetched once and served from the cached property thereafter.
    assert client.count("efetch") == 1


def test_read_structure_empty_without_statistics() -> None:
    # A run whose full XML carries no <Statistics> reports no structure.
    client = g.build_client()
    client._efetch[("sra", "full")] = (
        f'<EXPERIMENT_PACKAGE_SET><RUN accession="{g.SRR1}" total_spots="600"/>'
        "</EXPERIMENT_PACKAGE_SET>"
    )
    r = Run(g.SRR1, client=client)
    assert r.read_lengths == []
    assert r.n_reads is None
    assert r.read_structure == ""


def test_experiment_link_is_instance() -> None:
    r = Run(g.SRR1, client=g.build_client())
    assert r.experiment == Experiment(g.SRX)
    assert isinstance(r.experiment, Experiment)


def test_url() -> None:
    assert Run(g.SRR1).url == "https://www.ncbi.nlm.nih.gov/sra/?term=SRR9000001"
