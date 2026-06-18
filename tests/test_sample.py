"""Tests for labdata.geo.Sample."""

import pytest

from labdata.exceptions import AccessionError
from labdata.geo import Experiment, Platform, Sample, Series
from tests import _geodata as g


@pytest.mark.parametrize("bad", ["GSE131907", "GSM", "gsm1", "", "SRX1"])
def test_invalid_accession_raises(bad: str) -> None:
    with pytest.raises(AccessionError):
        Sample(bad)


def test_metadata() -> None:
    s = Sample(g.GSM1, client=g.build_client())
    assert s.uid == g.GSM1_UID
    assert s.title == "LUNG_N01"
    assert s.organism == "Homo sapiens"


def test_series_link_is_instance() -> None:
    s = Sample(g.GSM1, client=g.build_client())
    assert s.series == Series(g.GSE)
    assert isinstance(s.series, Series)


def test_platform_link_is_instance() -> None:
    s = Sample(g.GSM1, client=g.build_client())
    assert s.platform == Platform(g.GPL)
    assert isinstance(s.platform, Platform)


def test_experiments_are_instances() -> None:
    experiments = Sample(g.GSM1, client=g.build_client()).experiments
    assert experiments == [Experiment(g.SRX)]


def test_urls() -> None:
    s = Sample(g.GSM1)
    assert s.geo_url == "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM3827114"
    assert (
        s.supplementary_http_url
        == "https://ftp.ncbi.nlm.nih.gov/geo/samples/GSM3827nnn/GSM3827114/suppl/"
    )
