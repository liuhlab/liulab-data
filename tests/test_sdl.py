"""Tests for labdata.ncbi.sdl.SdlClient."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

import pytest

from labdata.exceptions import SdlError
from labdata.ncbi.sdl import SdlClient

# A trimmed but structurally faithful sdl/2/retrieve response for SRR20172067:
# one original-format ``TenX`` BAM plus the normalized ``sra`` archive file.
_SDL_JSON = {
    "version": "2",
    "result": [
        {
            "bundle": "SRR20172067",
            "status": 200,
            "msg": "ok",
            "files": [
                {
                    "object": "remote|85023129",
                    "accession": "SRR20172067",
                    "type": "TenX",
                    "name": "possorted_genome_bam_TC2_d15_1.bam",
                    "size": 17220510363,
                    "md5": "4cf6c049aa2a0693d44158b1b06ea679",
                    "modificationDate": "2022-07-14T06:00:26Z",
                    "locations": [
                        {
                            "service": "s3",
                            "region": "us-east-1",
                            "link": "https://sra-pub-src-2.s3.amazonaws.com/"
                            "SRR20172067/possorted_genome_bam_TC2_d15_1.bam.1",
                        }
                    ],
                },
                {
                    "object": "srapub|SRR20172067",
                    "accession": "SRR20172067",
                    "type": "sra",
                    "name": "SRR20172067",
                    "size": 7650138419,
                    "md5": "b2f5d7e58e8187771eb0470be2c1f3bf",
                    "modificationDate": "2022-07-14T06:04:38Z",
                    "locations": [
                        {
                            "service": "s3",
                            "region": "us-east-1",
                            "link": "https://sra-pub-run-odp.s3.amazonaws.com/"
                            "sra/SRR20172067/SRR20172067",
                        }
                    ],
                },
            ],
        }
    ],
}


class _FakeResponse:
    """A minimal stand-in for an ``http.client.HTTPResponse`` context manager."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def read(self) -> bytes:
        return self._body


def _patch_urlopen(monkeypatch: pytest.MonkeyPatch, body: bytes) -> list[str]:
    """Patch ``urlopen`` to return ``body``; return a list that records request URLs."""
    urls: list[str] = []

    def fake_urlopen(request: Any, *, timeout: float = 0) -> _FakeResponse:
        urls.append(request.full_url)
        return _FakeResponse(body)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return urls


def test_retrieve_parses_files(monkeypatch: pytest.MonkeyPatch) -> None:
    urls = _patch_urlopen(monkeypatch, json.dumps(_SDL_JSON).encode())
    files = SdlClient().retrieve("SRR20172067")

    assert [f.name for f in files] == [
        "possorted_genome_bam_TC2_d15_1.bam",
        "SRR20172067",
    ]
    tenx = files[0]
    assert tenx.type == "TenX"
    assert tenx.size == 17220510363
    assert tenx.md5 == "4cf6c049aa2a0693d44158b1b06ea679"
    assert tenx.is_sra_archive is False
    assert files[1].is_sra_archive is True
    assert tenx.locations[0].service == "s3"
    assert tenx.url == (
        "https://sra-pub-src-2.s3.amazonaws.com/SRR20172067/possorted_genome_bam_TC2_d15_1.bam.1"
    )
    # The request carries the accession and asks for the mirror locations.
    assert "acc=SRR20172067" in urls[0]
    assert "accept-alternate-locations=yes" in urls[0]


def test_retrieve_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    urls = _patch_urlopen(monkeypatch, json.dumps(_SDL_JSON).encode())
    client = SdlClient()
    client.retrieve("SRR20172067")
    client.retrieve("SRR20172067")
    assert len(urls) == 1  # second call served from the per-instance cache


def test_non_ok_status_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    body = json.dumps(
        {"version": "2", "result": [{"bundle": "SRR0", "status": 404, "msg": "not found"}]}
    ).encode()
    _patch_urlopen(monkeypatch, body)
    with pytest.raises(SdlError, match="404"):
        SdlClient().retrieve("SRR0")


def test_invalid_json_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_urlopen(monkeypatch, b"<html>not json</html>")
    with pytest.raises(SdlError, match="invalid JSON"):
        SdlClient().retrieve("SRR20172067")


def test_transport_error_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(request: Any, *, timeout: float = 0) -> Any:
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", boom)
    with pytest.raises(SdlError, match="request failed"):
        SdlClient().retrieve("SRR20172067")


@pytest.mark.network
def test_retrieve_live() -> None:
    files = SdlClient().retrieve("SRR20172067")
    originals = [f for f in files if not f.is_sra_archive]
    assert any(f.type == "TenX" and f.name.endswith(".bam") for f in originals)
    assert all(f.url.startswith(("https://", "s3://", "gs://")) for f in files)
