"""Tests for labdata.ncbi.entrez over a fake transport: no socket, real E-utilities XML."""

from __future__ import annotations

import gzip
import io
import socket
import threading
import time
import urllib.error
import urllib.parse
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import pytest

import labdata.ncbi.entrez as entrez_mod
from labdata.exceptions import EntrezError, PageNotFound
from labdata.ncbi._http import HttpTransport, RateLimiter
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient, _is_transient

CREDS = NcbiCredentials("t@t.org", "abc")

_ESEARCH = (
    '<?xml version="1.0" ?>\n<!DOCTYPE eSearchResult PUBLIC "-//NLM//DTD esearch 20060628//EN" '
    '"https://eutils.ncbi.nlm.nih.gov/eutils/dtd/20060628/esearch.dtd">\n'
    "<eSearchResult><Count>{n}</Count><RetMax>{n}</RetMax><RetStart>0</RetStart>"
    "<IdList>{ids}</IdList><TranslationSet/><QueryTranslation>x</QueryTranslation>"
    "</eSearchResult>"
)
_ESUMMARY = (
    '<?xml version="1.0" ?>\n<!DOCTYPE eSummaryResult PUBLIC "-//NLM//DTD esummary v1 20041029//EN"'
    ' "https://eutils.ncbi.nlm.nih.gov/eutils/dtd/20041029/esummary-v1.dtd">\n'
    "<eSummaryResult>{docs}</eSummaryResult>"
)
_DOCSUM = '<DocSum><Id>{uid}</Id><Item Name="title" Type="String">{title}</Item></DocSum>'
_ELINK = (
    '<?xml version="1.0" ?>\n<!DOCTYPE eLinkResult PUBLIC "-//NLM//DTD elink 20101123//EN" '
    '"https://eutils.ncbi.nlm.nih.gov/eutils/dtd/20101123/elink.dtd">\n'
    "<eLinkResult>{sets}</eLinkResult>"
)
_LINKSET = (
    "<LinkSet><DbFrom>gds</DbFrom><IdList><Id>{uid}</Id></IdList>"
    "<LinkSetDb><DbTo>sra</DbTo><LinkName>gds_sra</LinkName>{links}</LinkSetDb></LinkSet>"
)


def _esearch(*ids: str) -> bytes:
    return _ESEARCH.format(n=len(ids), ids="".join(f"<Id>{i}</Id>" for i in ids)).encode()


def _esummary(*docs: tuple[str, str]) -> bytes:
    return _ESUMMARY.format(docs="".join(_DOCSUM.format(uid=u, title=t) for u, t in docs)).encode()


def _elink(sets: dict[str, list[str]]) -> bytes:
    body = "".join(
        _LINKSET.format(uid=uid, links="".join(f"<Link><Id>{x}</Id></Link>" for x in links))
        for uid, links in sets.items()
    )
    return _ELINK.format(sets=body).encode()


@dataclass
class Reply:
    """A canned response; ``read`` serves the body in pieces, as a socket does."""

    body: bytes = b""
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    _stream: io.BytesIO | None = None
    reads: list[int | None] = field(default_factory=list)

    def getheader(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name, default)

    def read(self, amt: int | None = None) -> bytes:
        if self._stream is None:
            self._stream = io.BytesIO(self.body)
        self.reads.append(amt)
        return self._stream.read(amt)


@dataclass
class Call:
    method: str
    url: str
    params: dict[str, list[str]]
    timeout: float
    at: float
    headers: dict[str, str]


Answer = Reply | BaseException | Callable[["Call"], "Reply | BaseException"]


class FakeTransport:
    """Answers each request with the next canned answer; the last one repeats."""

    def __init__(self, *answers: Answer) -> None:
        self.answers = list(answers)
        self.calls: list[Call] = []
        self._lock = threading.Lock()

    @contextmanager
    def open(self, method, url, *, body, headers, timeout) -> Iterator[Reply]:
        params = urllib.parse.parse_qs(body.decode()) if body else {}
        call = Call(method, url, params, timeout, time.monotonic(), dict(headers))
        with self._lock:
            self.calls.append(call)
            answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(answer, Reply):
            answer = Reply(answer.body, answer.status, answer.headers)  # a fresh body per request
        elif not isinstance(answer, BaseException):
            answer = answer(call)
        if isinstance(answer, BaseException):
            raise answer
        yield answer


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record every backoff wait instead of sleeping."""
    waited: list[float] = []
    monkeypatch.setattr(entrez_mod.time, "sleep", waited.append)
    return waited


def _client(*answers: Answer, rate: float = 1000.0, inflight: int = 8) -> EntrezClient:
    limiter = RateLimiter(rate, inflight)
    return EntrezClient(CREDS, transport=FakeTransport(*answers), limiter=limiter)


def _calls(client: EntrezClient) -> list[Call]:
    transport = client._transport
    assert isinstance(transport, FakeTransport)
    return transport.calls


# ---------------------------------------------------------------- the E-utilities calls


def test_esearch_posts_with_identity_and_a_30s_timeout() -> None:
    client = _client(Reply(_esearch("200131907")))
    assert client.esearch(db="gds", term="GSE1[ACCN]") == ["200131907"]
    (call,) = _calls(client)
    assert call.method == "POST"
    assert call.url == "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    assert call.timeout == 30.0
    assert call.params["term"] == ["GSE1[ACCN]"]
    assert call.params["email"] == ["t@t.org"]
    assert call.params["api_key"] == ["abc"]
    assert call.params["tool"] == ["liulab-data"]


def test_esearch_is_cached_and_copied() -> None:
    client = _client(Reply(_esearch("7")))
    first = client.esearch(db="gds", term="x")
    first.append("mutated")
    assert client.esearch(db="gds", term="x") == ["7"]
    assert len(_calls(client)) == 1
    client.clear_cache()
    client.esearch(db="gds", term="x")
    assert len(_calls(client)) == 2


def test_esummary_returns_the_first_docsum_and_raises_when_empty() -> None:
    assert _client(Reply(_esummary(("1", "T")))).esummary("gds", "1")["title"] == "T"
    with pytest.raises(EntrezError, match="empty esummary"):
        _client(Reply(_esummary())).esummary("gds", "1")


def test_esummary_many_chunks_large_id_lists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(entrez_mod, "_ESUMMARY_CHUNK", 2)
    client = _client(Reply(_esummary(("1", "a"), ("2", "b"))), Reply(_esummary(("3", "c"))))
    docs = client.esummary_many("gds", ["1", "2", "3"])
    assert [d["title"] for d in docs] == ["a", "b", "c"]
    assert [c.params["id"] for c in _calls(client)] == [["1,2"], ["3"]]
    assert client.esummary_many("gds", []) == []
    assert len(_calls(client)) == 2


def test_elink_collects_linked_uids() -> None:
    client = _client(Reply(_elink({"1": ["9", "8"]})))
    assert client.elink(dbfrom="gds", db="sra", uid="1") == ["9", "8"]


def test_elink_many_sends_one_id_per_uid_in_one_request() -> None:
    client = _client(Reply(_elink({"1": ["9", "8"], "2": []})))
    result = client.elink_many(dbfrom="gds", db="sra", uids=["1", "2", "1"])
    assert result == {"1": ["9", "8"], "2": []}
    assert [c.params["id"] for c in _calls(client)] == [["1", "2"]]


def test_efetch_returns_text_cached_unless_refreshed() -> None:
    client = _client(Reply(b"Run,bases\nSRR1,10\n"))
    assert client.efetch(db="sra", ids=["1", "2"], rettype="runinfo") == "Run,bases\nSRR1,10\n"
    client.efetch(db="sra", ids=["1", "2"], rettype="runinfo")
    assert len(_calls(client)) == 1
    assert _calls(client)[0].params["id"] == ["1,2"]
    client.efetch(db="sra", ids=["1", "2"], rettype="runinfo", refresh=True)
    assert len(_calls(client)) == 2
    assert client.efetch(db="sra", ids=[], rettype="runinfo") == ""
    assert len(_calls(client)) == 2


def test_a_reply_that_will_not_parse_becomes_entrez_error() -> None:
    with pytest.raises(EntrezError, match="request failed"):
        _client(Reply(b"<html>not xml</html>")).esearch(db="gds", term="x")


# ---------------------------------------------------------------- retries and pacing


def test_is_transient_tells_a_dropped_response_from_a_bad_query() -> None:
    assert _is_transient(RuntimeError("NCBI ...: Response ended prematurely"))
    assert _is_transient(TimeoutError("timed out"))
    assert _is_transient(ConnectionResetError())
    assert _is_transient(urllib.error.HTTPError("u", 503, "busy", {}, None))  # type: ignore[arg-type]
    assert not _is_transient(urllib.error.HTTPError("u", 400, "bad", {}, None))  # type: ignore[arg-type]
    assert not _is_transient(RuntimeError("ID list is empty"))


def test_a_timed_out_request_is_retried(sleeps: list[float]) -> None:
    client = _client(TimeoutError("timed out"), Reply(_esearch("42")))
    assert client.esearch(db="gds", term="x") == ["42"]
    assert len(_calls(client)) == 2
    assert len(sleeps) == 1


def test_a_drop_that_never_clears_gives_up_as_entrez_error(sleeps: list[float]) -> None:
    client = _client(ConnectionResetError("reset"))
    with pytest.raises(EntrezError, match="NCBI E-utilities request failed: reset"):
        client.esearch(db="gds", term="x")
    assert len(_calls(client)) == entrez_mod._MAX_TRIES


def test_a_400_keeps_its_message_and_id_list_is_empty_is_not_retried(
    sleeps: list[float],
) -> None:
    client = _client(
        Reply(b"<ERROR>ID list is empty! Possibly it has no correct IDs.</ERROR>", 400)
    )
    with pytest.raises(EntrezError, match=r"request failed: HTTP Error 400: Bad Request"):
        client.efetch(db="sra", ids=["SRX0"], rettype="full", retmode="xml")
    assert len(_calls(client)) == 1


def test_a_429_waits_for_retry_after_then_halves_the_rate(sleeps: list[float]) -> None:
    client = _client(Reply(b"", 429, {"Retry-After": "7"}), Reply(_esearch("1")), rate=4.0)
    assert client.esearch(db="gds", term="x") == ["1"]
    assert 7.0 in sleeps
    assert client._limiter.rate == 2.0


def test_a_503_backs_off_with_jitter_and_halves_the_rate(
    sleeps: list[float], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(entrez_mod.random, "uniform", lambda lo, hi: lo)
    client = _client(Reply(b"", 503), Reply(_esearch("1")), rate=4.0)
    client.esearch(db="gds", term="x")
    assert sleeps == [0.5]  # half the first backoff step: the low end of the jitter
    assert client._limiter.rate == 2.0


def test_the_rate_never_exceeds_the_cap_under_concurrent_callers() -> None:
    rate, calls = 100.0, 40
    client = _client(Reply(_esearch("1")), rate=rate, inflight=8)
    threads = [threading.Thread(target=client.esearch, args=("gds", f"t{i}")) for i in range(calls)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    starts = sorted(c.at for c in _calls(client))
    assert len(starts) == calls
    window = 0.2
    busiest = max(sum(1 for t in starts if s <= t < s + window) for s in starts)
    assert busiest <= rate * window + 3  # slack for a thread that wakes late


def test_calls_in_flight_never_exceed_the_cap() -> None:
    open_now, peak, lock = [0], [0], threading.Lock()

    def slow(_call: Call) -> Reply:
        with lock:
            open_now[0] += 1
            peak[0] = max(peak[0], open_now[0])
        time.sleep(0.02)
        with lock:
            open_now[0] -= 1
        return Reply(_esearch("1"))

    client = _client(slow, inflight=3)
    threads = [threading.Thread(target=client.esearch, args=("gds", f"t{i}")) for i in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert peak[0] == 3


def test_without_an_api_key_the_rate_is_capped_at_ncbis_keyless_limit() -> None:
    limiter = RateLimiter(5.0, 8)
    EntrezClient(
        NcbiCredentials("t@t.org", None), transport=FakeTransport(Reply()), limiter=limiter
    )
    assert limiter.rate == 3.0


def test_the_real_transport_times_out_a_hung_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """A server that accepts and never answers costs one timeout, then the retry is answered."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen()
    port = server.getsockname()[1]
    held: list[socket.socket] = []

    def serve() -> None:
        hung, _ = server.accept()
        held.append(hung)  # accepted, never answered
        conn, _ = server.accept()
        conn.recv(65536)
        body = b"^SERIES = GSE1\n"
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s" % (len(body), body))
        conn.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    monkeypatch.setattr(entrez_mod, "_GEO_TIMEOUT", 0.2)
    monkeypatch.setattr(entrez_mod, "_backoff", lambda _attempt: 0.0)
    transport = HttpTransport()
    client = EntrezClient(CREDS, transport=transport, limiter=RateLimiter(1000.0, 8))
    try:
        assert client.fetch_text(f"http://127.0.0.1:{port}/GSE1") == "^SERIES = GSE1\n"
    finally:
        transport.close()
        thread.join(2)
        for sock in held:
            sock.close()
        server.close()


# ---------------------------------------------------------------- batched efetch


def _packages(known: set[str], left_out: set[str]) -> Callable[[Call], Reply]:
    """Answer an efetch with one line per known id, leaving ``left_out`` out the first time."""
    dropped: set[str] = set()

    def answer(call: Call) -> Reply:
        ids = call.params["id"][0].split(",")
        if not set(ids) & known:
            return Reply(b"<ERROR>ID list is empty! Possibly it has no correct IDs.</ERROR>", 400)
        out = []
        for i in ids:
            if i in left_out and i not in dropped:
                dropped.add(i)
            elif i in known:
                out.append(f"{i}:text")
        return Reply("\n".join(out).encode())

    return answer


def _split(reply: str) -> dict[str, str]:
    return dict(line.split(":", 1) for line in reply.splitlines() if line)


def test_efetch_records_re_asks_for_what_a_batch_left_out() -> None:
    client = _client(_packages({"SRX1", "SRX2", "SRX3"}, left_out={"SRX2"}))
    got = client.efetch_records("sra", ["SRX1", "SRX2", "SRX3"], rettype="full", split=_split)
    assert got.records == {"SRX1": "text", "SRX2": "text", "SRX3": "text"}
    assert got.missing == []
    assert [c.params["id"] for c in _calls(client)] == [["SRX1,SRX2,SRX3"], ["SRX2"]]


def test_an_id_ncbi_says_is_empty_is_missing_and_the_rest_are_kept(sleeps: list[float]) -> None:
    client = _client(_packages({"SRX1", "SRX3"}, left_out=set()))
    got = client.efetch_records("sra", ["SRX1", "SRX2", "SRX3"], rettype="full", split=_split)
    assert got.records == {"SRX1": "text", "SRX3": "text"}
    assert got.missing == ["SRX2"]
    assert got.failed == {}
    assert len(_calls(client)) == 2  # the batch, then one re-ask; the 400 is not retried


def test_a_batch_where_no_id_resolves_is_all_missing() -> None:
    client = _client(_packages(set(), left_out=set()))
    got = client.efetch_records("sra", ["SRX8", "SRX9"], rettype="full", split=_split)
    assert got.records == {}
    assert got.missing == ["SRX8", "SRX9"]
    assert len(_calls(client)) == 1


def test_a_failed_re_ask_never_drops_the_rest_of_the_batch(sleeps: list[float]) -> None:
    first = _packages({"SRX1", "SRX2"}, left_out={"SRX2"})
    client = _client(first, ConnectionResetError("reset"))
    got = client.efetch_records("sra", ["SRX1", "SRX2"], rettype="full", split=_split)
    assert got.records == {"SRX1": "text"}
    assert got.missing == []
    assert list(got.failed) == ["SRX2"]


# ---------------------------------------------------------------- the GEO site


def test_fetch_text_gets_a_page_once_with_a_15s_timeout() -> None:
    client = _client(Reply(b"^SAMPLE = GSM1\n"))
    assert client.fetch_text("https://www.ncbi.nlm.nih.gov/x") == "^SAMPLE = GSM1\n"
    assert client.fetch_text("https://www.ncbi.nlm.nih.gov/x") == "^SAMPLE = GSM1\n"
    (call,) = _calls(client)
    assert (call.method, call.url, call.timeout) == ("GET", "https://www.ncbi.nlm.nih.gov/x", 15.0)
    assert call.headers["User-Agent"] == "liulab-data (+https://github.com/liuhlab/liulab-data)"


def test_fetch_text_decompresses_a_gzip_file() -> None:
    client = _client(Reply(gzip.compress(b"^SERIES = GSE1\n")))
    assert client.fetch_text("https://x/GSE1_family.soft.gz") == "^SERIES = GSE1\n"


def test_fetch_text_signals_a_missing_page() -> None:
    with pytest.raises(PageNotFound, match="https://x/"):
        _client(Reply(b"gone", 404)).fetch_text("https://x/")


def test_download_streams_to_disk_in_chunks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(entrez_mod, "_CHUNK_BYTES", 4096)
    body = bytes(range(256)) * 64
    replies: list[Reply] = []

    def answer(_call: Call) -> Reply:
        replies.append(Reply(body))
        return replies[-1]

    client = _client(answer)
    dest = tmp_path / "GSE1_family.soft.gz"
    assert client.download("https://x/GSE1_family.soft.gz", dest) == len(body)
    assert dest.read_bytes() == body
    (reply,) = replies
    assert all(amt is not None and amt < len(body) for amt in reply.reads)
    assert not list(tmp_path.glob("*.part"))


def test_download_of_a_missing_file_raises_and_leaves_nothing(tmp_path: Path) -> None:
    with pytest.raises(PageNotFound):
        _client(Reply(b"gone", 404)).download("https://x/f", tmp_path / "f")
    assert list(tmp_path.iterdir()) == []
