"""Shared fixtures for the test suite.

The whole package funnels NCBI access through ``labdata.ncbi.EntrezClient``, so
tests substitute :class:`~tests._fakes.FakeEntrezClient` — a recording stand-in
seeded with canned responses — and never touch the network.
"""

from __future__ import annotations

import contextlib
import functools
import importlib
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest
import typer.main
import typer.testing

from labdata._http import RateLimiter
from labdata.papers import pubmed
from tests import _geodata as g
from tests import _slices, _snakemake
from tests._fakes import FakeEntrezClient

# Imported here, at collection, so a cold import is never inside a test's time limit.
for _heavy in ("duckdb", "pandas", "pyarrow.parquet", "typer.rich_utils"):
    importlib.import_module(_heavy)

#: Each setup tests share, by the fixture that hands it out: how it starts, and the wait for it.
_SHARED: dict[str, tuple[Callable[[], None], Callable[[], None]]] = {
    "built_store": (_slices.start_prebuild, _slices.prebuild),
    "snakemake": (_snakemake.start, _snakemake.ready),
}


@pytest.fixture(autouse=True, scope="session")
def _test_defaults() -> Generator[None]:
    """Run every test under :func:`tests._slices.defaults`, with the CLI built once.

    Typer turns the app into a Click command on every ``CliRunner.invoke``; a ``labdata``
    process does it once, and so does a test session.
    """
    with pytest.MonkeyPatch.context() as patch:
        _slices.defaults(patch)
        patch.setattr(typer.testing, "_get_command", functools.cache(typer.main.get_command))
        yield


@pytest.fixture(autouse=True)
def _own_id_converter_pace(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give each test a fast PMC ID converter pace of its own.

    The process shares one pace of three calls a second, so a test's first call would wait
    for the last test's.
    """
    monkeypatch.setattr(pubmed, "IDCONV_LIMITER", RateLimiter(1000.0, 1))


#: The ``workerinput`` key that names the folder every pytest-xdist worker builds the slices in.
_SLICES_FOLDER = "labdata_slices"


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node: Any) -> None:
    """Give each pytest-xdist worker the one folder the slices are built in, once for the run."""
    node.workerinput[_SLICES_FOLDER] = str(_slices.shared_folder())


def pytest_configure(config: pytest.Config) -> None:
    """In a pytest-xdist worker, build the slices in the folder the run shares."""
    folder = getattr(config, "workerinput", {}).get(_SLICES_FOLDER)
    if folder:
        _slices.share(Path(folder))


def pytest_collection_finish(session: pytest.Session) -> None:
    """Start each shared setup a selected test needs, in its own process, beside the tests."""
    for name, (start, _) in _SHARED.items():
        if any(name in getattr(item, "fixturenames", ()) for item in session.items):
            start()


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_protocol(item: pytest.Item) -> Generator[None, object, object]:
    """Wait for the shared setups a test needs before its time limit starts: not its cost.

    A setup that fails here fails again in its fixture, as the test's error.
    """
    for name, (_, wait) in _SHARED.items():
        if name in getattr(item, "fixturenames", ()):
            with contextlib.suppress(Exception):
                wait()
    return (yield)


def pytest_sessionfinish() -> None:
    """Remove the prebuilt slices and stop the Snakemake server."""
    _slices.remove_prebuilt()
    _snakemake.stop()


@pytest.fixture(scope="session")
def built_store() -> _slices.BuiltStore:
    """Return ``copy(path, slice_name)``: the store :func:`tests._slices.built_root` makes.

    Each slice is fetched and built once a session; every call copies it, so a test may
    change its copy.
    """
    _slices.prebuild()
    return _slices.copy_built


@pytest.fixture(scope="session")
def snakemake() -> _snakemake.Runner:
    """Return :func:`tests._snakemake.run`: ``snakemake argv`` from a server started once."""
    _snakemake.ready()
    return _snakemake.run


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
