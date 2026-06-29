"""Tests for labdata.geo.BioProject."""

import pytest

from labdata.exceptions import AccessionError, EntrezError
from labdata.geo import BioProject, Experiment, Series
from tests import _geodata as g


@pytest.mark.parametrize("bad", ["GSE131907", "PRJ", "PRJNA", "", "545296"])
def test_invalid_accession_raises(bad: str) -> None:
    with pytest.raises(AccessionError):
        BioProject(bad)


@pytest.mark.parametrize("good", ["PRJNA545296", "PRJEB12345", "PRJDB6789"])
def test_valid_accessions(good: str) -> None:
    assert BioProject(good).accession == good


def test_accession_is_normalized() -> None:
    assert BioProject("  prjna545296  ").accession == "PRJNA545296"


def test_repr() -> None:
    assert repr(BioProject(g.PRJNA)) == "BioProject('PRJNA545296')"


def test_metadata() -> None:
    bp = BioProject(g.PRJNA, client=g.build_client())
    assert bp.uid == g.BP_UID
    assert bp.title == "Single cell RNA sequencing of lung adenocarcinoma"
    assert bp.name == "Single cell RNA sequencing of lung adenocarcinoma"
    assert bp.description.startswith("We performed")
    assert bp.organism == "Homo sapiens"
    assert bp.data_type == "Transcriptome or Gene expression"
    assert bp.registration_date == "2019/05/29 00:00"
    assert bp.submitter == "The Catholic University of Korea"


def test_no_record_raises() -> None:
    bp = BioProject(g.PRJNA, client=g.build_client().but(esearch={}))
    with pytest.raises(EntrezError, match="no BioProject"):
        _ = bp.uid


def test_url() -> None:
    assert BioProject(g.PRJNA).url == "https://www.ncbi.nlm.nih.gov/bioproject/PRJNA545296"


def test_series_are_instances() -> None:
    series = BioProject(g.PRJNA, client=g.build_client()).series
    assert series == [Series(g.GSE)]
    assert isinstance(series[0], Series)


def test_series_use_one_batched_summary_and_seed() -> None:
    client = g.build_client()
    series = BioProject(g.PRJNA, client=client).series
    assert client.count("esummary_many") == 1
    before = len(client.calls)
    assert series[0].title.startswith("Single-cell")
    assert series[0].organism == "Homo sapiens"
    assert series[0].pubmed_id == "32385277"
    assert len(client.calls) == before  # served from the seeded gds summary


def test_experiments_are_instances() -> None:
    experiments = BioProject(g.PRJNA, client=g.build_client()).experiments
    assert experiments == [Experiment(g.SRX)]
    assert isinstance(experiments[0], Experiment)


def test_experiments_use_one_batched_summary() -> None:
    client = g.build_client()
    _ = BioProject(g.PRJNA, client=client).experiments
    assert client.count("esummary_many") == 1
    assert not any(name == "esummary" and args[0] == "sra" for name, args in client.calls)


def test_linked_instances_share_client() -> None:
    client = g.build_client()
    bp = BioProject(g.PRJNA, client=client)
    assert bp.series[0].client is client
    assert bp.experiments[0].client is client


def test_lazy_and_cached() -> None:
    client = g.build_client()
    bp = BioProject(g.PRJNA, client=client)
    assert client.calls == []  # construction does no network I/O

    assert bp.uid == g.BP_UID
    assert bp.uid == g.BP_UID  # cached: not re-fetched
    assert client.count("esearch") == 1

    _ = bp.title
    _ = bp.organism
    assert client.count("esummary") == 1  # the bioproject summary is fetched once and shared
