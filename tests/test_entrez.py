"""Tests for labdata.ncbi.entrez — the Bio.Entrez wrapper.

``Bio.Entrez`` itself is monkeypatched: its request functions return a dummy
context-manager handle and ``Entrez.read`` returns canned parsed structures, so
no network or XML parsing happens here.
"""

import pytest

import labdata.ncbi.entrez as entrez_mod
from labdata.exceptions import EntrezError
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient

CREDS = NcbiCredentials("t@t.org", "abc")


class _Handle:
    """Minimal context-manager stand-in for an E-utilities handle."""

    def __enter__(self) -> "_Handle":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False


@pytest.fixture
def patch_entrez(monkeypatch: pytest.MonkeyPatch):
    """Return a helper that installs a fake function + read result on Bio.Entrez."""

    def _patch(func_name: str, read_result: object) -> None:
        monkeypatch.setattr(entrez_mod.Entrez, func_name, lambda **_kw: _Handle())
        monkeypatch.setattr(entrez_mod.Entrez, "read", lambda _handle: read_result)

    return _patch


def test_sets_credentials_on_entrez() -> None:
    EntrezClient(CREDS)
    assert entrez_mod.Entrez.email == "t@t.org"
    assert entrez_mod.Entrez.api_key == "abc"


def test_esearch_returns_idlist(patch_entrez) -> None:
    patch_entrez("esearch", {"IdList": ["200131907"]})
    assert EntrezClient(CREDS).esearch(db="gds", term="x") == ["200131907"]


def test_esearch_empty(patch_entrez) -> None:
    patch_entrez("esearch", {"IdList": []})
    assert EntrezClient(CREDS).esearch(db="gds", term="x") == []


def test_esummary_returns_first_docsum(patch_entrez) -> None:
    patch_entrez("esummary", [{"title": "T", "PubMedIds": ["123"]}])
    assert EntrezClient(CREDS).esummary(db="gds", uid="1") == {
        "title": "T",
        "PubMedIds": ["123"],
    }


def test_esummary_document_summary_set(patch_entrez) -> None:
    # bioproject and other v2.0 databases nest summaries under DocumentSummarySet.
    record = {"DocumentSummarySet": {"DocumentSummary": [{"Project_Acc": "PRJNA545296"}]}}
    patch_entrez("esummary", record)
    assert EntrezClient(CREDS).esummary(db="bioproject", uid="545296") == {
        "Project_Acc": "PRJNA545296"
    }


def test_esummary_empty_raises(patch_entrez) -> None:
    patch_entrez("esummary", [])
    with pytest.raises(EntrezError, match="empty esummary"):
        EntrezClient(CREDS).esummary(db="gds", uid="1")


def test_esummary_empty_document_summary_set_raises(patch_entrez) -> None:
    patch_entrez("esummary", {"DocumentSummarySet": {"DocumentSummary": []}})
    with pytest.raises(EntrezError, match="empty esummary"):
        EntrezClient(CREDS).esummary(db="bioproject", uid="1")


def test_elink_collects_linked_uids(patch_entrez) -> None:
    record = [{"LinkSetDb": [{"Link": [{"Id": "999"}, {"Id": "1000"}]}]}]
    patch_entrez("elink", record)
    assert EntrezClient(CREDS).elink(dbfrom="gds", db="sra", uid="1") == ["999", "1000"]


def test_elink_no_links(patch_entrez) -> None:
    patch_entrez("elink", [{"LinkSetDb": []}])
    assert EntrezClient(CREDS).elink(dbfrom="gds", db="sra", uid="1") == []


def test_read_failure_becomes_entrez_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(entrez_mod.Entrez, "esearch", lambda **_kw: _Handle())

    def _boom(_handle: object) -> object:
        raise ValueError("bad xml")

    monkeypatch.setattr(entrez_mod.Entrez, "read", _boom)
    with pytest.raises(EntrezError, match="request failed"):
        EntrezClient(CREDS).esearch(db="gds", term="x")
