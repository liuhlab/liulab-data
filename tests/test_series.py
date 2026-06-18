"""Tests for labdata.geo.Series — lazy GEO Series resolution via a fake client."""

from collections.abc import Callable

import pytest

from labdata.exceptions import AccessionError, EntrezError
from labdata.geo import Series

GSE = "GSE131907"
UID = "200131907"
SRA_UID = "5921017"

_SUMMARY = {
    "title": "Single-cell landscape of lung adenocarcinoma",
    "summary": "An scRNA-seq atlas ...",
    "taxon": "Homo sapiens",
    "PubMedIds": ["32385277"],
    "Samples": [
        {"Accession": "GSM3828672", "Title": "P0006"},
        {"Accession": "GSM3828673", "Title": "P0008"},
    ],
}


def _build(make_client: Callable[..., object], **over: object) -> object:
    cfg: dict[str, object] = {
        "esearch": {f"{GSE}[ACCN] AND gse[ETYP]": [UID]},
        "esummary": {
            ("gds", UID): _SUMMARY,
            ("sra", SRA_UID): {"ExpXml": '<Experiment acc="SRX5921017"/>'},
        },
        "elink": {("gds", "sra", UID): [SRA_UID]},
    }
    cfg.update(over)
    return make_client(**cfg)


@pytest.mark.parametrize("bad", ["GSM3828672", "GSE", "gse123", "GSE12a", "", "foo"])
def test_invalid_accession_raises(bad: str) -> None:
    with pytest.raises(AccessionError):
        Series(bad)


def test_non_string_accession_raises() -> None:
    with pytest.raises(AccessionError):
        Series(123)  # type: ignore[arg-type]


def test_repr() -> None:
    assert repr(Series(GSE)) == "Series('GSE131907')"


def test_uid_resolution(make_client: Callable[..., object]) -> None:
    assert Series(GSE, client=_build(make_client)).uid == UID


def test_no_record_raises(make_client: Callable[..., object]) -> None:
    s = Series(GSE, client=_build(make_client, esearch={}))
    with pytest.raises(EntrezError, match="no GEO Series"):
        _ = s.uid


def test_metadata_fields(make_client: Callable[..., object]) -> None:
    s = Series(GSE, client=_build(make_client))
    assert s.title.startswith("Single-cell")
    assert s.organism == "Homo sapiens"
    assert s.pubmed_id == "32385277"


def test_pubmed_id_absent(make_client: Callable[..., object]) -> None:
    summary = {**_SUMMARY, "PubMedIds": []}
    s = Series(GSE, client=_build(make_client, esummary={("gds", UID): summary}))
    assert s.pubmed_id is None


def test_samples(make_client: Callable[..., object]) -> None:
    assert Series(GSE, client=_build(make_client)).samples == ["GSM3828672", "GSM3828673"]


def test_experiments(make_client: Callable[..., object]) -> None:
    assert Series(GSE, client=_build(make_client)).experiments == ["SRX5921017"]


def test_experiments_deduplicates(make_client: Callable[..., object]) -> None:
    client = _build(
        make_client,
        elink={("gds", "sra", UID): [SRA_UID, "999"]},
        esummary={
            ("gds", UID): _SUMMARY,
            ("sra", SRA_UID): {"ExpXml": '<Experiment acc="SRX5921017"/>'},
            ("sra", "999"): {"ExpXml": 'acc="SRX5921017" and acc="SRX0000001"'},
        },
    )
    assert Series(GSE, client=client).experiments == ["SRX5921017", "SRX0000001"]


def test_experiments_empty_without_sra(make_client: Callable[..., object]) -> None:
    assert Series(GSE, client=_build(make_client, elink={})).experiments == []


def test_lazy_and_cached(make_client: Callable[..., object]) -> None:
    client = _build(make_client)
    s = Series(GSE, client=client)
    assert client.calls == []  # construction does no network I/O

    assert s.uid == UID
    assert s.uid == UID  # cached_property: second access does not re-call
    assert client.count("esearch") == 1

    _ = s.title
    _ = s.samples
    _ = s.pubmed_id
    assert client.count("esummary") == 1  # the gds summary is fetched once and shared
