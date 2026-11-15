"""Shared fixtures for the test suite.

The whole package funnels NCBI access through ``labdata.ncbi.EntrezClient``, so
tests substitute :class:`~tests._fakes.FakeEntrezClient` — a recording stand-in
seeded with canned responses — and never touch the network.
"""

from __future__ import annotations

import compileall
import gzip
import importlib
import importlib.util
import re
from collections.abc import Callable, Iterator
from importlib import metadata
from pathlib import Path
from typing import Any

import pytest

from labdata.store import extract
from tests import _geodata as g
from tests import _slices, _snakemake
from tests._fakes import FakeEntrezClient

# Imported here, at collection, so a cold import is never inside a test's time limit.
for _heavy in ("duckdb", "pandas", "pyarrow.parquet", "typer.rich_utils"):
    importlib.import_module(_heavy)


def _compile(name: str) -> None:
    """Compile a distribution and every one it requires, as a fresh environment lacks bytecode.

    The tests that start a Snakemake process would otherwise pay for it inside their time limit.
    """
    seen: set[str] = set()
    todo = [name]
    while todo:
        key = re.sub(r"[-_.]+", "-", todo.pop()).lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            dist = metadata.distribution(key)
        except metadata.PackageNotFoundError:
            continue
        todo += [re.split(r"[^A-Za-z0-9_.-]", r, maxsplit=1)[0] for r in dist.requires or []]
        for file in dist.files or []:
            if file.suffix == ".py":
                compileall.compile_file(Path(str(dist.locate_file(file))), quiet=2)


if not Path(
    importlib.util.cache_from_source(
        str(metadata.distribution("snakemake").locate_file("snakemake/cli.py"))
    )
).is_file():
    _compile("snakemake")


@pytest.fixture(autouse=True, scope="session")
def _serial_workers() -> Iterator[None]:
    """Default every parallel step to one worker, so no test starts Ray unasked."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("SLURM_CPUS_ON_NODE", "1")
        yield


class Decompressor:
    """Stands in for rapidgzip or pigz: a gzip file's bytes on ``stdout``, then ``code``."""

    def __init__(self, command: list[str], code: int = 0) -> None:
        self.stdout = gzip.open(command[-1], "rb")  # noqa: SIM115 - the caller closes the pipe
        self._code = code

    def kill(self) -> None:
        pass

    def wait(self) -> int:
        return self._code


@pytest.fixture(autouse=True)
def _no_decompressor_binaries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Decompress snapshots in Python at the extract's subprocess seam."""
    monkeypatch.setattr(extract, "_popen", Decompressor)


def _prebuild() -> None:
    """Build every thin slice once, under the test defaults no fixture has set yet."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("SLURM_CPUS_ON_NODE", "1")
        patch.setattr(extract, "_popen", Decompressor)
        _slices.prebuild()


def pytest_collection_finish(session: pytest.Session) -> None:
    """Build the slices, and start the Snakemake server, when a selected test needs them.

    A shared fixture's setup counts toward the time limit of the first test that uses it.
    """
    if any("built_store" in getattr(item, "fixturenames", ()) for item in session.items):
        _prebuild()
    if any(hasattr(getattr(item, "module", None), "_snakemake") for item in session.items):
        _snakemake.start()


def pytest_sessionfinish() -> None:
    """Remove the prebuilt slices and stop the Snakemake server."""
    _slices.remove_prebuilt()
    _snakemake.stop()


@pytest.fixture(scope="session")
def built_store() -> Callable[[Path, str], Path]:
    """Return ``copy(path, slice_name)``: the store :func:`tests._slices.built_root` makes.

    Each slice is fetched and built once a session; every call copies it, so a test may
    change its copy.
    """
    _prebuild()
    return _slices.copy_built


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
