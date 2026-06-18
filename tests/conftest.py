"""Shared fixtures for the test suite.

The whole package funnels NCBI access through ``labdata.ncbi.EntrezClient``, so
tests substitute :class:`~tests._fakes.FakeEntrezClient` — a recording stand-in
seeded with canned responses — and never touch the network.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from tests._fakes import FakeEntrezClient


@pytest.fixture
def make_client() -> Callable[..., FakeEntrezClient]:
    """Return a factory that builds a :class:`FakeEntrezClient` from canned data."""

    def _make(**kwargs: Any) -> FakeEntrezClient:
        return FakeEntrezClient(**kwargs)

    return _make
