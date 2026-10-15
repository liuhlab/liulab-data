"""Building SRA experiments and runs from ``EXPERIMENT_PACKAGE`` XML.

The fixtures under ``tests/data/sra_package/`` are real ``efetch db=sra rettype=full``
responses, one experiment per file.
"""

from __future__ import annotations

from functools import cached_property
from pathlib import Path

import pytest

from labdata import experiments_for
from labdata.exceptions import LabdataError
from labdata.geo import Experiment, Run
from labdata.geo.sra_records import experiments_from_package_xml
from tests._fakes import ExplodingEntrezClient, FakeEntrezClient

_DATA = Path(__file__).parent / "data" / "sra_package"

#: Properties that look files up in the SRA Data Locator, not in the package XML.
_SDL_PROPERTIES = {"files", "original_files"}


def _package(accession: str) -> str:
    return (_DATA / f"{accession}.xml").read_text()


def _public_properties(obj: object) -> list[str]:
    return sorted(
        name
        for name in dir(type(obj))
        if not name.startswith("_")
        and isinstance(getattr(type(obj), name), property | cached_property)
        and name not in _SDL_PROPERTIES
    )


def test_one_package_gives_its_experiment_and_runs() -> None:
    (experiment,) = experiments_from_package_xml(
        _package("SRX24283130"), client=ExplodingEntrezClient()
    )
    assert experiment.accession == "SRX24283130"
    assert experiment.title == "single nucleus RNA-seq of Adult C. elegans neurons"
    assert experiment.study == "SRP502277"
    assert experiment.organism == "Caenorhabditis elegans"
    assert experiment.instrument_model == "Illumina NovaSeq 6000"
    assert experiment.uid is None
    (run,) = experiment.runs
    assert run == Run("SRR28716558")
    assert (run.total_spots, run.total_bases, run.is_public) == (455092911, 55521335142, True)
    assert (run.read_lengths, run.n_reads, run.read_structure) == ([28, 94], 2, "28+94")
    assert run.experiment is experiment


def test_a_geo_package_without_a_scientific_name_takes_the_pool_organism() -> None:
    (experiment,) = experiments_from_package_xml(
        _package("SRX018673"), client=ExplodingEntrezClient()
    )
    assert experiment.title == "GSM484820: Lung_pooled 3 control pigs (C)"
    assert experiment.study == "SRP002215"
    assert experiment.organism == "Sus scrofa"
    assert experiment.instrument_model == "Illumina Genome Analyzer"
    (run,) = experiment.runs
    assert run.accession == "SRR039169"
    assert (run.total_spots, run.total_bases) == (7767249, 271853715)
    assert run.read_structure == "35"


def test_many_packages_give_one_experiment_each_in_order() -> None:
    accessions = ["SRX018673", "SRX018710", *(f"SRX2428313{i}" for i in range(6))]
    xml = "".join(_package(accession) for accession in accessions)
    experiments = experiments_from_package_xml(xml + _package("SRX018673"))
    assert [e.accession for e in experiments] == accessions


def test_every_property_is_read_without_the_client() -> None:
    xml = "".join(_package(a) for a in ["SRX018673", "SRX018710", "SRX24283130"])
    for experiment in experiments_from_package_xml(xml, client=ExplodingEntrezClient()):
        for name in _public_properties(experiment):
            getattr(experiment, name)
        assert experiment.runs
        for run in experiment.runs:
            for name in _public_properties(run):
                getattr(run, name)
            assert run.experiment.runs == experiment.runs


def test_text_that_is_not_xml_raises() -> None:
    with pytest.raises(LabdataError, match="not SRA package XML"):
        experiments_from_package_xml("<EXPERIMENT_PACKAGE>")


def test_the_built_experiment_equals_a_handle_by_accession() -> None:
    (experiment,) = experiments_from_package_xml(_package("SRX018710"))
    assert experiment == Experiment("SRX018710")
    assert experiment.study == "SRP002222"


_PRJNA1027859 = [f"SRX2428313{i}" for i in range(6)]


def _fields(experiment: Experiment) -> list[object]:
    runs = [
        (r.accession, r.total_spots, r.total_bases, r.is_public, r.read_lengths, r.n_reads)
        for r in experiment.runs
    ]
    return [
        experiment.accession,
        experiment.title,
        experiment.study,
        experiment.organism,
        experiment.instrument_model,
        runs,
    ]


def _live_client(**drops: int) -> FakeEntrezClient:
    return FakeEntrezClient(
        esearch={"PRJNA1027859": ["1027859"]},
        elink={("bioproject", "sra", "1027859"): _PRJNA1027859},
        packages={a: _package(a) for a in _PRJNA1027859},
        drops=drops,
    )


def test_the_live_path_and_the_parser_give_the_same_fields() -> None:
    live = experiments_for("PRJNA1027859", client=_live_client())
    stored = experiments_from_package_xml(
        "".join(_package(a) for a in _PRJNA1027859), client=ExplodingEntrezClient()
    )
    assert [_fields(e) for e in live] == [_fields(e) for e in stored]
    assert [e.accession for e in live] == _PRJNA1027859


def test_a_short_batch_fetches_only_the_dropped_ids_again() -> None:
    client = _live_client(SRX24283132=1, SRX24283134=2)
    experiments = experiments_for("PRJNA1027859", client=client)
    assert sorted(e.accession for e in experiments) == _PRJNA1027859
    requests = [args[1] for name, args in client.calls if name == "efetch"]
    assert requests == [
        tuple(_PRJNA1027859),
        ("SRX24283132", "SRX24283134"),
        ("SRX24283134",),
    ]


def test_a_batch_still_short_after_the_retries_raises() -> None:
    client = _live_client(SRX24283133=3)
    with pytest.raises(LabdataError, match="no package for: SRX24283133"):
        experiments_for("PRJNA1027859", client=client)


def test_a_short_batch_of_uids_is_fetched_again() -> None:
    client = FakeEntrezClient(
        esearch={"SRP502277": ["1", "2"]},
        packages={"1": _package("SRX24283130"), "2": _package("SRX24283131")},
        drops={"2": 1},
    )
    experiments = experiments_for("SRP502277", client=client)
    assert [e.accession for e in experiments] == ["SRX24283130", "SRX24283131"]
    assert client.count("efetch") == 2
