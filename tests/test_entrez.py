"""Tests for labdata.ncbi.entrez — the Bio.Entrez wrapper.

``Bio.Entrez`` itself is monkeypatched: its request functions return a dummy
context-manager handle and ``Entrez.read`` returns canned parsed structures, so
no network or XML parsing happens here.
"""

import http.client
import urllib.error

import pytest

import labdata.ncbi.entrez as entrez_mod
from labdata.exceptions import EntrezError
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient, _is_transient

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


@pytest.fixture
def count_entrez(monkeypatch: pytest.MonkeyPatch):
    """Install a request counter on a Bio.Entrez function; return the call log.

    Each invocation records its keyword arguments so tests can assert both how
    many requests were issued and with what ids.
    """

    def _install(func_name: str, read_result: object) -> list[dict[str, object]]:
        calls: list[dict[str, object]] = []

        def _func(**kw: object) -> _Handle:
            calls.append(kw)
            return _Handle()

        monkeypatch.setattr(entrez_mod.Entrez, func_name, _func)
        monkeypatch.setattr(entrez_mod.Entrez, "read", lambda _handle: read_result)
        return calls

    return _install


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


def test_esummary_many_returns_all_docsums(patch_entrez) -> None:
    patch_entrez("esummary", [{"Id": "1", "x": "a"}, {"Id": "2", "x": "b"}])
    result = EntrezClient(CREDS).esummary_many(db="sra", uids=["1", "2"])
    assert [d["x"] for d in result] == ["a", "b"]


def test_esummary_many_document_summary_set(patch_entrez) -> None:
    record = {"DocumentSummarySet": {"DocumentSummary": [{"Project_Acc": "PRJNA1"}]}}
    patch_entrez("esummary", record)
    result = EntrezClient(CREDS).esummary_many(db="bioproject", uids=["1"])
    assert result == [{"Project_Acc": "PRJNA1"}]


def test_esummary_many_empty_issues_no_request(count_entrez) -> None:
    calls = count_entrez("esummary", [])
    assert EntrezClient(CREDS).esummary_many(db="sra", uids=[]) == []
    assert calls == []  # no network for an empty id list


def test_esummary_many_chunks_large_id_lists(count_entrez, monkeypatch) -> None:
    monkeypatch.setattr(entrez_mod, "_ESUMMARY_CHUNK", 2)
    calls = count_entrez("esummary", [{"Id": "x"}])
    EntrezClient(CREDS).esummary_many(db="sra", uids=["1", "2", "3", "4", "5"])
    # 5 ids in chunks of 2 -> 3 requests, each a comma-joined id string.
    assert [kw["id"] for kw in calls] == ["1,2", "3,4", "5"]


def test_esearch_is_cached(count_entrez) -> None:
    calls = count_entrez("esearch", {"IdList": ["7"]})
    client = EntrezClient(CREDS)
    assert client.esearch(db="gds", term="x") == ["7"]
    assert client.esearch(db="gds", term="x") == ["7"]  # served from cache
    assert len(calls) == 1


def test_esummary_is_cached(count_entrez) -> None:
    calls = count_entrez("esummary", [{"title": "T"}])
    client = EntrezClient(CREDS)
    client.esummary(db="gds", uid="1")
    client.esummary(db="gds", uid="1")
    assert len(calls) == 1


def test_elink_is_cached(count_entrez) -> None:
    calls = count_entrez("elink", [{"LinkSetDb": [{"Link": [{"Id": "9"}]}]}])
    client = EntrezClient(CREDS)
    client.elink(dbfrom="gds", db="sra", uid="1")
    client.elink(dbfrom="gds", db="sra", uid="1")
    assert len(calls) == 1


class _TextHandle:
    """Context-manager handle whose ``read`` returns canned bytes/text."""

    def __init__(self, data: object) -> None:
        self._data = data

    def __enter__(self) -> "_TextHandle":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def read(self) -> object:
        return self._data


def test_efetch_returns_text(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        entrez_mod.Entrez, "efetch", lambda **_kw: _TextHandle(b"Run,bases\nSRR1,10\n")
    )
    out = EntrezClient(CREDS).efetch(db="sra", ids=["1"], rettype="runinfo")
    assert out == "Run,bases\nSRR1,10\n"


def test_efetch_empty_ids_issues_no_request(count_entrez) -> None:
    calls = count_entrez("efetch", _TextHandle("x"))
    assert EntrezClient(CREDS).efetch(db="sra", ids=[], rettype="runinfo") == ""
    assert calls == []


def test_efetch_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        entrez_mod.Entrez, "efetch", lambda **kw: calls.append(kw) or _TextHandle("data")
    )
    client = EntrezClient(CREDS)
    client.efetch(db="sra", ids=["1", "2"], rettype="runinfo")
    client.efetch(db="sra", ids=["1", "2"], rettype="runinfo")
    assert len(calls) == 1
    assert calls[0]["id"] == "1,2"


def test_clear_cache_forces_refetch(count_entrez) -> None:
    calls = count_entrez("esearch", {"IdList": ["7"]})
    client = EntrezClient(CREDS)
    client.esearch(db="gds", term="x")
    client.clear_cache()
    client.esearch(db="gds", term="x")
    assert len(calls) == 2


def test_cached_list_is_copied(count_entrez) -> None:
    count_entrez("esearch", {"IdList": ["7"]})
    client = EntrezClient(CREDS)
    first = client.esearch(db="gds", term="x")
    first.append("mutated")
    assert client.esearch(db="gds", term="x") == ["7"]  # cache untouched by caller


def test_read_failure_becomes_entrez_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(entrez_mod.Entrez, "esearch", lambda **_kw: _Handle())

    def _boom(_handle: object) -> object:
        raise ValueError("bad xml")

    monkeypatch.setattr(entrez_mod.Entrez, "read", _boom)
    with pytest.raises(EntrezError, match="request failed"):
        EntrezClient(CREDS).esearch(db="gds", term="x")


# ---------------------------------------------------------------- transient-retry


def test_is_transient_tells_a_dropped_response_from_a_bad_query() -> None:
    """Connection-level failures retry; a query NCBI rejects does not."""
    # transient: the request reached NCBI and the response is what broke
    assert _is_transient(RuntimeError("NCBI ...: Response ended prematurely"))
    assert _is_transient(RuntimeError("Read failed: EOF (the other side ... closed connection)"))
    assert _is_transient(http.client.IncompleteRead(b"partial"))
    assert _is_transient(ConnectionResetError())
    assert _is_transient(urllib.error.URLError("connection refused"))
    assert _is_transient(urllib.error.HTTPError("u", 503, "busy", {}, None))  # type: ignore[arg-type]
    assert _is_transient(urllib.error.HTTPError("u", 429, "slow down", {}, None))  # type: ignore[arg-type]
    # permanent: our request is the problem, so retrying only hammers NCBI
    assert not _is_transient(urllib.error.HTTPError("u", 400, "bad", {}, None))  # type: ignore[arg-type]
    assert not _is_transient(RuntimeError("ID list is empty"))


def test_a_transient_drop_is_retried_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """NCBI closing the response mid-parse is retried, not surfaced as a failure."""
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(entrez_mod.Entrez, "esearch", lambda **_kw: _Handle())
    reads = {"n": 0}

    def _read(_handle: object) -> object:
        reads["n"] += 1
        if reads["n"] < 3:
            raise RuntimeError("NCBI E-utilities: Response ended prematurely")
        return {"IdList": ["42"]}

    monkeypatch.setattr(entrez_mod.Entrez, "read", _read)
    assert EntrezClient(CREDS).esearch(db="gds", term="x") == ["42"]
    assert reads["n"] == 3  # two drops, third try wins


def test_a_permanent_error_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """A query NCBI will never accept fails fast, instead of hammering it _MAX_TRIES times."""
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(entrez_mod.Entrez, "esearch", lambda **_kw: _Handle())
    reads = {"n": 0}

    def _read(_handle: object) -> object:
        reads["n"] += 1
        raise RuntimeError("ID list is empty")  # logical, not a dropped connection

    monkeypatch.setattr(entrez_mod.Entrez, "read", _read)
    with pytest.raises(EntrezError):
        EntrezClient(CREDS).esearch(db="gds", term="x")
    assert reads["n"] == 1


def test_a_drop_that_never_clears_gives_up_as_entrez_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persistently failing request still terminates — after exactly _MAX_TRIES attempts."""
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(entrez_mod.Entrez, "esearch", lambda **_kw: _Handle())
    reads = {"n": 0}

    def _read(_handle: object) -> object:
        reads["n"] += 1
        raise RuntimeError("Response ended prematurely")

    monkeypatch.setattr(entrez_mod.Entrez, "read", _read)
    with pytest.raises(EntrezError, match="request failed"):
        EntrezClient(CREDS).esearch(db="gds", term="x")
    assert reads["n"] == entrez_mod._MAX_TRIES


def test_efetch_retries_a_dropped_response(monkeypatch: pytest.MonkeyPatch) -> None:
    """The exact production failure: an efetch response drops mid-read. Retried, not fatal."""
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)
    state = {"reads": 0}

    class _FetchHandle:
        def __enter__(self) -> "_FetchHandle":
            return self

        def __exit__(self, *_exc: object) -> bool:
            return False

        def read(self) -> bytes:
            state["reads"] += 1
            if state["reads"] < 2:
                raise http.client.IncompleteRead(b"partial")
            return b"Run,ReleaseDate\nSRR1,2024\n"

    monkeypatch.setattr(entrez_mod.Entrez, "efetch", lambda **_kw: _FetchHandle())
    out = EntrezClient(CREDS).efetch(db="sra", ids=["1"], rettype="runinfo")
    assert "SRR1" in out
    assert state["reads"] == 2  # first read dropped, second succeeded
