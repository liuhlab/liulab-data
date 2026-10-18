"""``scripts/store/benchmark.py``: time the spec's queries on a build, one process each."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tests._slices import built_root

SCRIPT = Path(__file__).parents[1] / "scripts" / "store" / "benchmark.py"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def _files(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix() for p in root.rglob("*")}


def test_it_reports_each_query_and_each_table_and_writes_nothing_in_the_store(
    tmp_path: Path,
) -> None:
    root = built_root(tmp_path / "store", "geo_slice")
    before = _files(root)
    out = tmp_path / "result.json"

    done = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            str(root),
            "--sizes",
            "1,5",
            "--seed",
            "7",
            "--out",
            str(out),
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=tmp_path,
    )

    assert done.returncode == 0, done.stderr
    result = json.loads(out.read_text())
    queries = {q["query"]: q for q in result["queries"]}
    assert list(queries) == [
        "get 1",
        "get 5",
        "table GEO Sample",
        "links 5 Series",
        "readme 1",
        "readme 2",
        "readme 3",
        "raw 5",
    ]
    held = pq.read_table(root / "builds" / "b1" / "holdings.parquet").to_pylist()
    samples = sum(row["type"] == "GEO Sample" for row in held)
    # Every sampled accession is held.
    assert [queries[q]["rows"] for q in ("get 1", "get 5", "raw 5")] == [1, 5, 5]
    assert queries["table GEO Sample"]["rows"] == samples
    assert queries["readme 1"]["rows"] == 1
    assert all(
        q["wall_s"] > 0 and q["cpu_s"] > 0 and q["peak_rss_mib"] > 0 for q in queries.values()
    )
    tables = {t["file"]: t for t in result["tables"]}
    assert tables["records/geo-sample.parquet"]["rows"] == samples
    assert tables["links_by_from.parquet"]["row_groups"] >= 1
    assert "readme 1" in done.stdout
    assert "records/geo-sample.parquet" in done.stdout
    assert _files(root) == before
