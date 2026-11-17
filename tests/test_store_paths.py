"""The store layout: every path in a store root is declared state, rebuildable or scratch."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import pytest

from labdata.store import extract, gcs, paths
from tests._fake_gcs import FakeGcs
from tests._slices import BuiltStore


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def test_the_copy_sends_the_state_files_and_the_readme_frees_the_scratch_ones_not_extracts(
    built_store: BuiltStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = built_store(tmp_path / "store", "geo_slice")
    files = [p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()]
    state = {p for p in files if paths.kind(p) is paths.Kind.STATE}
    fake = FakeGcs.install(monkeypatch, tmp_path / "gcs", "gcs:bucket/store")

    gcs.copy(root, "gcs:bucket/store", full=True)

    # GCS holds the state files, and the copy record names each but the two records.
    assert set(fake.files()) == state | {paths.COPY_RECORD.rel, paths.STATUS.rel}
    assert set(json.loads(paths.COPY_RECORD.at(root).read_text(encoding="utf-8"))) == state
    readme = paths.README.at(root).read_text(encoding="utf-8")
    cleanup = readme.split("\n## Clean up\n", 1)[1].split("\n## ", 1)[0]
    listed = re.findall(r"^- `([^`]+)`", cleanup, re.MULTILINE)
    assert sorted(listed) == sorted(paths.declared(paths.Kind.SCRATCH))
    # Deleting what the README frees, and all of work/, keeps every extract.
    shutil.rmtree(root / "work")
    shutil.rmtree(root / "support", ignore_errors=True)
    for source in extract.SOURCES:
        extract.done_dir(root, source)
