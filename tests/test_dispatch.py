"""Tests for labdata.experiments_for — the any-accession -> experiments dispatcher."""

from __future__ import annotations

import pytest

from labdata import experiments_for
from labdata.exceptions import AccessionError
from labdata.geo import Experiment
from tests import _geodata as g


@pytest.mark.parametrize(
    "accession",
    [g.GSE, g.GSM1, g.PRJNA, g.SRX, g.SRR1],
)
def test_every_link_based_accession_resolves_to_the_experiment(accession: str) -> None:
    """A GEO Series/Sample, a BioProject, an experiment and a run all reach SRX5921017."""
    experiments = experiments_for(accession, client=g.build_client())
    assert [e.accession for e in experiments] == [g.SRX]
    assert all(isinstance(e, Experiment) for e in experiments)


def test_a_study_accession_resolves_by_sra_search() -> None:
    """A study (``SRP``) has no record class, so it is resolved by searching ``sra``."""
    client = g.build_client().but(esearch={"SRP200000": [g.SRA_UID]})
    experiments = experiments_for("SRP200000", client=client)
    assert [e.accession for e in experiments] == [g.SRX]
    # the search hits ``sra`` with the accession as the term, not a GEO/link database
    assert ("esearch", ("sra", "SRP200000")) in client.calls


def test_a_biosample_accession_resolves_by_sra_search() -> None:
    """A BioSample (``SAMN``) is likewise resolved through the ``sra`` search route."""
    client = g.build_client().but(esearch={"SAMN34081621": [g.SRA_UID]})
    experiments = experiments_for("SAMN34081621", client=client)
    assert [e.accession for e in experiments] == [g.SRX]


def test_the_resolved_experiments_share_the_given_client() -> None:
    """Every experiment reuses the passed client, so downstream reads issue no new auth."""
    client = g.build_client()
    for e in experiments_for(g.GSE, client=client):
        assert e.client is client


def test_the_accession_is_normalized() -> None:
    """Case and surrounding whitespace do not matter."""
    experiments = experiments_for("  srx5921017  ", client=g.build_client())
    assert [e.accession for e in experiments] == [g.SRX]


def test_a_record_with_no_sra_data_is_an_empty_list_not_an_error() -> None:
    """Emptiness is a legitimate answer; the caller decides what it means, not this resolver."""
    client = g.build_client().but(esearch={})
    assert experiments_for("SRP999999", client=client) == []


@pytest.mark.parametrize("bad", ["banana", "GPL16791", "", "GSE"])
def test_an_unresolvable_accession_raises(bad: str) -> None:
    with pytest.raises(AccessionError):
        experiments_for(bad, client=g.build_client())
