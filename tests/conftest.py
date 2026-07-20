"""Shared fixtures for the test suite.

The whole package funnels NCBI access through ``labdata.ncbi.EntrezClient``, so
tests substitute :class:`~tests._fakes.FakeEntrezClient` — a recording stand-in
seeded with canned responses — and never touch the network.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from tests import _geodata as g
from tests._fakes import FakeEntrezClient


@pytest.fixture
def make_client() -> Callable[..., FakeEntrezClient]:
    """Return a factory that builds a :class:`FakeEntrezClient` from canned data."""

    def _make(**kwargs: Any) -> FakeEntrezClient:
        return FakeEntrezClient(**kwargs)

    return _make


@pytest.fixture
def fake_sdl(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every bare ``Run(...)`` build a :class:`FakeSdlClient` (no SDL network).

    The default SRA download path never touches SDL, but the *original-format* path
    resolves ``Run.original_files``; the stage subprocesses (and ``fake_snakemake``)
    construct ``Run`` without an injected client, so patch the ``SdlClient`` the
    ``_base`` module instantiates to the synthetic listing.
    """
    from labdata.geo import _base

    monkeypatch.setattr(_base, "SdlClient", g.build_sdl_client)
