"""The paper store layout: every top-level file is declared state or scratch."""

from __future__ import annotations

from pathlib import Path

import pytest

from labdata.papers import lifecycle, open_paper_store, paths
from tests._fake_gcs import FakeGcs
from tests.test_papers_daily import (
    PDF,
    DailyWeb,
    Headed,
    _isolated,  # noqa: F401 - autouse: the cache, no sleeps
    daily,
    paper,
    paper_store,
    use,
)


def test_every_top_level_file_a_pass_and_a_copy_leave_is_declared_state_or_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    link = "https://www.biorxiv.org/c.pdf"
    root = paper_store(tmp_path, [paper("10.1101/c", pdf=False, hosts=(link,))])
    use(monkeypatch, DailyWeb(hosts={link: Headed(PDF, 200)}), [])
    daily(root)
    FakeGcs.install(monkeypatch, tmp_path / "gcs", "gcs:bucket/papers")
    lifecycle.copy(open_paper_store(root), "gcs:bucket/papers", full=True)

    kinds = {p.name: paths.kind(p.name) for p in root.iterdir()}

    assert set(kinds.values()) <= {paths.Kind.STATE, paths.Kind.SCRATCH}
    assert kinds[".copied-files.parquet"] is paths.Kind.STATE
    assert kinds[".lock"] is paths.Kind.SCRATCH
    assert kinds["papers"] is paths.Kind.STATE
