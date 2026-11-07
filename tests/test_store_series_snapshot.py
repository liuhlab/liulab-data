"""The ``geo-series`` snapshot: a patch of the previous one, or a full dump, over a fake NCBI."""

from __future__ import annotations

import gzip
import json
import threading
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

import labdata.ncbi.entrez as entrez_mod
from labdata._http import RateLimiter
from labdata.exceptions import LabdataError
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.store import provenance, series_snapshot
from tests.test_store_refresh_fetch import _ESEARCH, _Reply

_GSE = 200_000_000


@dataclass
class Gds:
    """GEO's Series in ``gds``: the ``gse[ETYP]`` list, the ``UDAT`` search and ``esummary``."""

    titles: dict[int, str]
    updated: set[int] = field(default_factory=set)
    failing_start: int | None = None
    extra_count: int = 0
    silent: dict[int, int] = field(default_factory=dict)
    terms: list[str] = field(default_factory=list)
    summarized: list[int] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @contextmanager
    def open(self, method, url, *, body, headers, timeout) -> Iterator[_Reply]:
        params = {k: v[0] for k, v in urllib.parse.parse_qs(body.decode()).items()}
        with self._lock:
            yield self._answer(url, params)

    def _answer(self, url: str, params: dict[str, str]) -> _Reply:
        if url.endswith("esummary.fcgi"):
            uids = [int(u) for u in params["id"].split(",")]
            self.summarized += uids
            got = [u for u in uids if u in self.titles and not self._silent(u)]
            result: dict[str, object] = {"uids": [str(u) for u in got]}
            result |= {str(u): self._summary(u) for u in got}
            return _Reply(json.dumps({"header": {}, "result": result}).encode())
        term, start = params["term"], int(params.get("retstart", 0))
        self.terms.append(term)
        if start == self.failing_start:
            return _Reply(b"<html>Internal Server Error</html>", 500)
        page = int(params["retmax"])
        if "[UDAT]" in term:
            ids = sorted(self.updated)[start : start + page]
            xml = "".join(f"<Id>{u}</Id>" for u in ids)
            return _Reply(
                _ESEARCH.format(n=len(self.updated), m=len(ids), s=start, ids=xml).encode()
            )
        listed = sorted(self.titles, reverse=True)[start : start + page]
        count = len(self.titles) + self.extra_count
        reply = {"esearchresult": {"count": str(count), "idlist": [str(u) for u in listed]}}
        return _Reply(json.dumps(reply).encode())

    def _silent(self, uid: int) -> bool:
        """Leave ``uid`` out of this reply if it is to go unanswered more times."""
        left = self.silent.get(uid, 0)
        self.silent[uid] = left - 1
        return left > 0

    def _summary(self, uid: int) -> dict[str, object]:
        gse = uid - _GSE
        return {"uid": str(uid), "accession": f"GSE{gse}", "title": self.titles[uid], "gse": gse}


@pytest.fixture(autouse=True)
def _no_waits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)


def _client(gds: Gds) -> EntrezClient:
    creds = NcbiCredentials("t@t.org", "abc")
    return EntrezClient(creds, transport=gds, limiter=RateLimiter(1000.0, 8))


def _uids(*numbers: int) -> list[int]:
    return [_GSE + n for n in numbers]


def _lines(folder: Path) -> list[str]:
    path = folder / series_snapshot.FILE
    return gzip.decompress(path.read_bytes()).decode().splitlines()


def test_a_patch_fetches_new_and_updated_series_and_equals_a_full_dump(tmp_path: Path) -> None:
    gds = Gds(dict.fromkeys(_uids(1, 2, 3, 4, 5), "v1"))
    series_snapshot.download(tmp_path / "snapshots" / "geo-series" / "2026-10-01", _client(gds))
    # A month on: GSE2 withdrawn, GSE3 revised, GSE7 and GSE6 released.
    now = dict.fromkeys(_uids(1, 3, 4, 5, 6, 7), "v1") | {_GSE + 3: "v2"}
    gds = Gds(now, updated=set(_uids(3, 6, 7)))

    patched = tmp_path / "snapshots" / "geo-series" / "2026-11-01"
    done = series_snapshot.download(patched, _client(gds))

    assert sorted(gds.summarized) == _uids(3, 6, 7)
    # the change window starts at the previous snapshot's date, less the overlap
    assert "gse[ETYP] AND 2026/09/28:3000[UDAT]" in gds.terms
    assert (done.patched_from, done.fetched, done.dropped, done.records) == ("2026-10-01", 3, 1, 6)
    full = tmp_path / "full" / "snapshots" / "geo-series" / "2026-11-01"
    series_snapshot.download(full, _client(Gds(now)), full=True)
    assert _lines(patched) == _lines(full)
    assert [json.loads(line)["accession"] for line in _lines(patched)] == [
        "GSE1", "GSE3", "GSE4", "GSE5", "GSE6", "GSE7",
    ]  # fmt: skip
    assert json.loads(_lines(patched)[1])["title"] == "v2"
    assert provenance.verify(tmp_path) == provenance.verify(tmp_path / "full") == []
    readme = (patched / "README.md").read_text(encoding="utf-8")
    assert "Patched from the 2026-10-01 snapshot" in readme


_SIX = dict.fromkeys(_uids(1, 2, 3, 4, 5, 6), "v1")


@pytest.mark.parametrize(
    "fault",
    [
        pytest.param({"failing_start": 2}, id="a page fails"),
        pytest.param({"extra_count": 1}, id="fewer UIDs than the count"),
        pytest.param({"titles": dict.fromkeys(_uids(1, 2), "v1")}, id="most Series dropped"),
    ],
)
def test_a_short_or_failed_listing_fails_and_writes_no_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: dict[str, Any]
) -> None:
    series_snapshot.download(tmp_path / "2026-10-01", _client(Gds(_SIX)))
    monkeypatch.setattr(series_snapshot, "PAGE", 2)
    monkeypatch.setattr(series_snapshot, "DROP_FLOOR", 1)
    gds = Gds(**{"titles": _SIX, **fault})

    with pytest.raises(LabdataError):
        series_snapshot.download(tmp_path / "2026-11-01", _client(gds))

    assert not (tmp_path / "2026-11-01" / series_snapshot.FILE).exists()
    assert gds.summarized == []


def _manifest(folder: Path) -> dict[str, Any]:
    return json.loads((folder / "manifest.json").read_text(encoding="utf-8"))


def test_a_series_esummary_leaves_out_once_is_asked_again(tmp_path: Path) -> None:
    series_snapshot.download(tmp_path / "2026-10-01", _client(Gds(_SIX)))
    gds = Gds(_SIX | {_GSE + 3: "v2"}, updated={_GSE + 3}, silent={_GSE + 3: 1})

    done = series_snapshot.download(tmp_path / "2026-11-01", _client(gds))

    assert gds.summarized == _uids(3, 3)
    assert json.loads(_lines(tmp_path / "2026-11-01")[2])["title"] == "v2"
    assert (done.fetched, done.unanswered) == (1, 0)
    assert _manifest(tmp_path / "2026-11-01")["unanswered"] == []


def test_series_left_unanswered_keep_their_old_row_and_the_next_patch_asks_again(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    series_snapshot.download(tmp_path / "2026-10-01", _client(Gds(_SIX)))
    now = _SIX | {_GSE + 3: "v2", _GSE + 4: "v2", _GSE + 7: "v1"}
    always = {_GSE + 3: 2, _GSE + 7: 2}
    gds = Gds(now, updated=set(_uids(3, 4, 7)), silent=always)

    with caplog.at_level("WARNING"):
        done = series_snapshot.download(tmp_path / "2026-11-01", _client(gds))

    lines = [json.loads(line) for line in _lines(tmp_path / "2026-11-01")]
    assert [(r["accession"], r["title"]) for r in lines] == [
        ("GSE1", "v1"), ("GSE2", "v1"), ("GSE3", "v1"), ("GSE4", "v2"), ("GSE5", "v1"),
        ("GSE6", "v1"),
    ]  # fmt: skip
    manifest = _manifest(tmp_path / "2026-11-01")
    assert manifest["unanswered"] == _uids(3, 7)
    assert (manifest["fetched"], done.fetched, done.unanswered) == (1, 1, 2)
    assert "2 Series left unanswered" in caplog.text

    gds = Gds(now)  # GEO updates nothing more
    series_snapshot.download(tmp_path / "2026-12-01", _client(gds))

    assert sorted(gds.summarized) == _uids(3, 7)
    lines = [json.loads(line) for line in _lines(tmp_path / "2026-12-01")]
    assert lines[2]["title"] == "v2"
    assert lines[-1]["accession"] == "GSE7"
    assert _manifest(tmp_path / "2026-12-01")["unanswered"] == []
