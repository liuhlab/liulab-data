"""Tests for labdata.geo.Series — lazy resolution + instance-returning links."""

import pytest

import labdata.geo._web as web_mod
from labdata.exceptions import AccessionError, EntrezError
from labdata.geo import BioProject, Experiment, Platform, Run, Sample, Series
from tests import _geodata as g


@pytest.mark.parametrize("bad", ["GSM3828672", "GSE", "gse123", "GSE12a", "", "foo"])
def test_invalid_accession_raises(bad: str) -> None:
    with pytest.raises(AccessionError):
        Series(bad)


def test_non_string_accession_raises() -> None:
    with pytest.raises(AccessionError):
        Series(123)  # type: ignore[arg-type]


def test_repr() -> None:
    assert repr(Series(g.GSE)) == "Series('GSE131907')"


def test_uid_and_metadata() -> None:
    s = Series(g.GSE, client=g.build_client())
    assert s.uid == g.GSE_UID
    assert s.title.startswith("Single-cell")
    assert s.summary.startswith("An scRNA-seq")
    assert s.organism == "Homo sapiens"
    assert s.pubmed_id == "32385277"


def test_no_record_raises() -> None:
    s = Series(g.GSE, client=g.build_client().but(esearch={}))
    with pytest.raises(EntrezError, match="no GEO Series"):
        _ = s.uid


def test_urls() -> None:
    s = Series(g.GSE)
    assert s.url == "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE131907"
    assert (
        s.supplementary_http_url
        == "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE131nnn/GSE131907/suppl/"
    )


def test_samples_are_instances() -> None:
    samples = Series(g.GSE, client=g.build_client()).samples
    assert samples == [Sample(g.GSM1), Sample(g.GSM2)]
    assert all(isinstance(s, Sample) for s in samples)


def test_platforms_are_instances() -> None:
    platforms = Series(g.GSE, client=g.build_client()).platforms
    assert platforms == [Platform(g.GPL)]
    assert isinstance(platforms[0], Platform)


def test_experiments_are_instances() -> None:
    experiments = Series(g.GSE, client=g.build_client()).experiments
    assert experiments == [Experiment(g.SRX)]
    assert isinstance(experiments[0], Experiment)


def test_experiments_use_one_batched_summary() -> None:
    client = g.build_client()
    _ = Series(g.GSE, client=client).experiments
    assert client.count("esummary_many") == 1
    # the per-UID sra esummary loop is gone
    assert not any(name == "esummary" and args[0] == "sra" for name, args in client.calls)


def test_linked_experiment_is_seeded() -> None:
    client = g.build_client()
    experiment = Series(g.GSE, client=client).experiments[0]
    before = len(client.calls)
    assert experiment.title == "scRNA-seq of LUNG_N01"
    assert experiment.instrument_model == "Illumina HiSeq 2500"
    assert experiment.runs == [Run(g.SRR1), Run(g.SRR2)]
    assert experiment.runs[0].total_spots == 600
    assert len(client.calls) == before  # served entirely from the seeded summary


def test_linked_instances_share_client() -> None:
    client = g.build_client()
    s = Series(g.GSE, client=client)
    assert s.samples[0].client is client
    assert s.platforms[0].client is client


def test_bioprojects_are_instances() -> None:
    bioprojects = Series(g.GSE, client=g.build_client()).bioprojects
    assert bioprojects == [BioProject(g.PRJNA)]
    assert isinstance(bioprojects[0], BioProject)


def test_bioprojects_use_one_batched_summary_and_seed() -> None:
    client = g.build_client()
    bioprojects = Series(g.GSE, client=client).bioprojects
    assert client.count("esummary_many") == 1
    before = len(client.calls)
    assert bioprojects[0].title == "Single cell RNA sequencing of lung adenocarcinoma"
    assert bioprojects[0].organism == "Homo sapiens"
    assert len(client.calls) == before  # served from the seeded bioproject summary


def test_supplementary_files_lists_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, str] = {}

    def fake_list(url: str) -> list[str]:
        seen["url"] = url
        return ["GSE131907_matrix.txt.gz", "GSE131907_meta.csv"]

    monkeypatch.setattr(web_mod, "list_directory", fake_list)
    s = Series(g.GSE, client=g.build_client())
    assert s.supplementary_files == ["GSE131907_matrix.txt.gz", "GSE131907_meta.csv"]
    assert seen["url"] == s.supplementary_http_url


def test_supplementary_file_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        web_mod, "list_directory", lambda _url: ["GSE131907_matrix.txt.gz", "GSE131907_meta.csv"]
    )
    s = Series(g.GSE, client=g.build_client())
    base = s.supplementary_http_url
    assert s.supplementary_file_urls == [
        base + "GSE131907_matrix.txt.gz",
        base + "GSE131907_meta.csv",
    ]


def test_lazy_and_cached() -> None:
    client = g.build_client()
    s = Series(g.GSE, client=client)
    assert client.calls == []  # construction does no network I/O

    assert s.uid == g.GSE_UID
    assert s.uid == g.GSE_UID  # cached: not re-fetched
    assert client.count("esearch") == 1

    _ = s.title
    _ = s.samples
    _ = s.pubmed_id
    assert client.count("esummary") == 1  # the gds summary is fetched once and shared
