"""A GEO sample's SRA experiments: its SOFT ``Sample_relation`` lines joined with ``elink``.

``elink`` gds -> sra misses some links that the SOFT record carries, and carries a few
that SOFT lacks, so the live path reads both.
"""

from __future__ import annotations

from pathlib import Path

from labdata import experiments_for
from labdata.geo.geo_records import Sample
from labdata.geo.sra_records import Experiment
from tests._fakes import FakeEntrezClient


def _soft_url(accession: str) -> str:
    return (
        "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"
        f"?acc={accession}&targ=gsm&form=text&view=brief"
    )


def _sample_soft(gsm: str, *srx: str) -> str:
    lines = [f"^SAMPLE = {gsm}", f"!Sample_geo_accession = {gsm}"]
    lines += [f"!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term={x}" for x in srx]
    lines.append("!Sample_relation = BioSample: https://www.ncbi.nlm.nih.gov/biosample/SAMN1")
    return "\n".join(lines) + "\n"


_DATA = Path(__file__).parent / "data" / "sra_package"


def _package(srx: str) -> str:
    fixture = _DATA / f"{srx}.xml"
    if fixture.exists():
        return fixture.read_text()
    return f'<EXPERIMENT_PACKAGE><EXPERIMENT accession="{srx}"/></EXPERIMENT_PACKAGE>'


def test_a_link_only_soft_carries_is_found() -> None:
    client = FakeEntrezClient(
        esearch={"GSM1000001[ACCN] AND gsm[ETYP]": ["301"]},
        pages={_soft_url("GSM1000001"): _sample_soft("GSM1000001", "SRX681985")},
        packages={"SRX681985": _package("SRX681985")},
    )
    assert Sample("GSM1000001", client=client).experiments == [Experiment("SRX681985")]


def test_a_link_only_elink_carries_is_kept() -> None:
    client = FakeEntrezClient(
        esearch={"GSM5339608[ACCN] AND gsm[ETYP]": ["305339608"]},
        packages={"9001": _package("SRX11032311")},
        elink={("gds", "sra", "305339608"): ["9001"]},
        pages={_soft_url("GSM5339608"): _sample_soft("GSM5339608")},
    )
    assert Sample("GSM5339608", client=client).experiments == [Experiment("SRX11032311")]


def test_a_sample_with_several_experiments_lists_each_once() -> None:
    client = FakeEntrezClient(
        esearch={"GSM484820[ACCN] AND gsm[ETYP]": ["300484820"]},
        packages={
            "18673": _package("SRX018673"),
            "SRX018710": _package("SRX018710"),
            "SRX681985": _package("SRX681985"),
        },
        elink={("gds", "sra", "300484820"): ["18673"]},
        pages={_soft_url("GSM484820"): _sample_soft("GSM484820", "SRX018673", "SRX018710")},
    )
    experiments = Sample("GSM484820", client=client).experiments
    assert experiments == [Experiment("SRX018673"), Experiment("SRX018710")]
    assert [e.study for e in experiments] == ["SRP002215", "SRP002222"]


def test_a_sample_with_no_experiment_is_empty() -> None:
    client = FakeEntrezClient(
        esearch={"GSM1000002[ACCN] AND gsm[ETYP]": ["302"]},
        pages={_soft_url("GSM1000002"): _sample_soft("GSM1000002")},
    )
    assert Sample("GSM1000002", client=client).experiments == []


def test_a_series_joins_its_samples_soft_in_one_request() -> None:
    series_soft = _sample_soft("GSM484820", "SRX018673", "SRX018710") + _sample_soft(
        "GSM1000001", "SRX681985"
    )
    client = FakeEntrezClient(
        esearch={"GSE19456[ACCN] AND gse[ETYP]": ["200019456"]},
        packages={
            "18673": _package("SRX018673"),
            "SRX018710": _package("SRX018710"),
            "SRX681985": _package("SRX681985"),
        },
        elink={("gds", "sra", "200019456"): ["18673"]},
        pages={_soft_url("GSE19456"): series_soft},
    )
    experiments = experiments_for("GSE19456", client=client)
    assert experiments == [
        Experiment("SRX018673"),
        Experiment("SRX018710"),
        Experiment("SRX681985"),
    ]
    assert client.count("fetch_text") == 1
    # The two SOFT-only experiments come in one batched package fetch.
    assert ("efetch", ("sra", ("SRX018710", "SRX681985"), "full")) in client.calls
    assert client.count("efetch") == 2
