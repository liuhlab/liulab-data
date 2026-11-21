"""Live checks of every declared vocabulary's addresses, release and prefix.

Marked ``network`` and deselected by default; run with ``pixi run test-network -k ontology``.
Each address gets one small request, and the OBO Foundry registry is read once.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from labdata._http import HttpTransport
from labdata.ontology import fetch
from labdata.ontology.update import current_release
from labdata.ontology.vocabularies import VOCABULARIES, ReaderKind, release_of

pytestmark = pytest.mark.network

REGISTRY = "https://obofoundry.org/registry/ontologies.jsonld"


class _Counted:
    """The live transport, counting the body bytes each request reads."""

    def __init__(self) -> None:
        self._web = HttpTransport()
        self.given: dict[str, int] = {}

    @contextmanager
    def open(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> Iterator[Any]:
        with self._web.open(method, url, body=body, headers=headers, timeout=timeout) as response:
            read = response.read

            def counted(amt: int | None = None) -> bytes:
                chunk = read(amt)
                self.given[url] = self.given.get(url, 0) + len(chunk)
                return chunk

            response.read = counted
            yield response


def test_every_declared_address_answers_and_states_its_release(tmp_path: Path) -> None:
    web = _Counted()
    for vocab in VOCABULARIES:
        release = current_release(vocab, transport=web)
        assert release == release_of(release), vocab.name
        addresses = set(vocab.downloads)
        if vocab.reader is ReaderKind.TAXDUMP:
            addresses.add(f"{vocab.url}.md5")
        # The release check already reached its own address.
        for url in sorted(addresses - {vocab.version_url}):
            assert fetch.read_head(url, transport=web, size=1), url
    assert max(web.given.values()) <= fetch.HEAD_BYTES

    fetch.download(REGISTRY, tmp_path / "registry.jsonld", transport=web)
    registry = json.loads((tmp_path / "registry.jsonld").read_text(encoding="utf-8"))
    prefixes = {entry["id"]: entry.get("preferredPrefix") for entry in registry["ontologies"]}
    for vocab in VOCABULARIES:
        if vocab.reader is ReaderKind.OBO:
            assert vocab.prefixes == (f"{prefixes[vocab.name]}:",), vocab.name
