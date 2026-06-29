"""Tests for labdata.geo.Platform."""

import pytest

from labdata.exceptions import AccessionError
from labdata.geo import Platform
from tests import _geodata as g


@pytest.mark.parametrize("bad", ["GSE131907", "GPL", "", "SRX1"])
def test_invalid_accession_raises(bad: str) -> None:
    with pytest.raises(AccessionError):
        Platform(bad)


def test_accession_is_normalized() -> None:
    assert Platform("  gpl1234  ").accession == "GPL1234"


def test_metadata() -> None:
    p = Platform(g.GPL, client=g.build_client())
    assert p.uid == g.GPL_UID
    assert p.title == "Illumina HiSeq 2500 (Homo sapiens)"
    assert p.organism == "Homo sapiens"
    assert p.sample_count == 373734


def test_urls() -> None:
    p = Platform(g.GPL)
    assert p.url == "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GPL16791"
    assert (
        p.supplementary_http_url
        == "https://ftp.ncbi.nlm.nih.gov/geo/platforms/GPL16nnn/GPL16791/suppl/"
    )
