"""Time the store's agreed queries on its current build, each in its own process.

Usage::

    pixi run python scripts/store/benchmark.py ROOT [--seed 0] [--sizes 1,1000,100000]
        [--out RESULT.json]

It samples held accessions with the seed, runs each query in a fresh Python process and
prints one row per query: rows returned, wall time and that process's peak resident memory.
Then it prints each build table's file size, rows and row groups. The store is only read;
temporary files go in a system temp folder, and output only to stdout and ``--out``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

#: The record type and fields of the table query.
TABLE_TYPE = "GEO Sample"
TABLE_FIELDS = ("Sample_title", "Sample_organism_ch1", "Sample_library_strategy")


def build_dir(root: Path) -> Path:
    """Return the current build folder of the store at ``root``."""
    from labdata.store import open_store

    store = open_store(root)
    return store.root / "builds" / str(store.settings["build"])


def readme_queries() -> list[str]:
    """Return the DuckDB examples of the store README, as written there."""
    import importlib.resources

    text = importlib.resources.files("labdata.store").joinpath("store_readme.md").read_text()
    return re.findall(r'duckdb\.sql\(f"""(.*?)"""\)', text, flags=re.DOTALL)


def sample(build: Path, sizes: list[int], seed: int, tmp: Path) -> dict[str, list[str]]:
    """Pick the held accessions each query reads, the same for the same seed and build."""
    import duckdb

    from labdata.store.layout import HOLDINGS, sql_string

    con = duckdb.connect(config={"temp_directory": str(tmp / "duckdb")})
    holdings = sql_string(str(HOLDINGS.path(build)))

    def pick(n: int, where: str = "TRUE") -> list[str]:
        rows = con.execute(
            f"SELECT accession FROM read_parquet({holdings}) WHERE {where} "
            "ORDER BY hash(accession || ?), accession LIMIT ?",
            [str(seed), n],
        ).fetchall()
        return [r[0] for r in rows]

    mixed = pick(max(sizes))
    middle = sizes[len(sizes) // 2] if len(sizes) > 1 else sizes[0]
    return {"mixed": mixed, "series": pick(middle, "type = 'GEO Series'")}


def run_query(name: str, root: Path, samples: dict[str, list[str]], tmp: Path) -> int:
    """Run one query by ``name`` and return the rows it returned."""
    from labdata.store import open_store

    store = open_store(root)
    kind, _, arg = name.partition(" ")
    if kind == "get":
        found = store.open_many(samples["mixed"][: int(arg)])
        return sum(not isinstance(v, Exception) for v in found.values())
    if kind == "raw":
        return len(store.raw(samples["mixed"][: int(arg)]))
    if kind == "table":
        return store.table(TABLE_TYPE, list(TABLE_FIELDS)).num_rows
    if kind == "links":
        samples_of = store.follow(samples["series"], "sample")
        gsm = {a for accs in samples_of.values() for a in accs}
        srx = {a for accs in store.follow(gsm, "SRA").values() for a in accs} if gsm else set()
        runs = {a for accs in store.follow(srx, "run").values() for a in accs} if srx else set()
        return len(runs)
    if kind == "readme":
        import duckdb

        build = str(store.root / "builds" / str(store.settings["build"]))
        sql = readme_queries()[int(arg) - 1].format(build=build)
        con = duckdb.connect(config={"temp_directory": str(tmp / "duckdb")})
        return len(con.sql(sql).fetchall())
    raise ValueError(f"unknown query {name!r}")


def query_names(sizes: list[int], series: int) -> list[str]:
    """Return the queries to run, in order."""
    middle = sizes[len(sizes) // 2] if len(sizes) > 1 else sizes[0]
    return [
        *(f"get {n}" for n in sizes),
        f"table {TABLE_TYPE}",
        f"links {series} Series",
        *(f"readme {i}" for i in range(1, len(readme_queries()) + 1)),
        f"raw {middle}",
    ]


def _own_usage() -> dict[str, float]:
    """Return this process's own peak memory and bytes read, where Linux reports them."""
    usage: dict[str, float] = {}
    proc = Path("/proc/self")
    if (proc / "status").exists():
        # VmHWM is this process's own peak; ru_maxrss also counts the parent's at fork.
        hwm = re.search(r"^VmHWM:\s+(\d+) kB", (proc / "status").read_text(), flags=re.M)
        if hwm:
            usage["peak_rss_mib"] = round(int(hwm.group(1)) / 1024, 1)
        rchar = re.search(r"^rchar: (\d+)", (proc / "io").read_text(), flags=re.M)
        if rchar:
            usage["read_mib"] = round(int(rchar.group(1)) / 2**20, 1)
    return usage


def measure(name: str, root: Path, samples_file: Path, tmp: Path) -> dict[str, Any]:
    """Run one query in a child process; return its rows, wall and CPU time, and peak memory."""
    child = subprocess.Popen(
        [sys.executable, __file__, str(root), "--one", name, "--samples", str(samples_file)],
        stdout=subprocess.PIPE,
        cwd=tmp,
        text=True,
    )
    assert child.stdout is not None
    output = child.stdout.read()
    _, status, usage = os.wait4(child.pid, 0)
    child.returncode = os.waitstatus_to_exitcode(status)
    if child.returncode != 0:
        raise RuntimeError(f"query {name!r} failed with exit code {child.returncode}")
    # ru_maxrss is in KiB on Linux and in bytes on macOS; the child's own figure wins.
    peak = usage.ru_maxrss / (1024 * 1024 if sys.platform == "darwin" else 1024)
    cpu = usage.ru_utime + usage.ru_stime
    return {
        "query": name,
        "peak_rss_mib": round(peak, 1),
        **json.loads(output),
        "cpu_s": round(cpu, 2),
    }


def table_stats(build: Path) -> list[dict[str, Any]]:
    """Return each Parquet file's size, rows and row groups."""
    import pyarrow.parquet as pq

    out = []
    for path in sorted(build.rglob("*.parquet")):
        meta = pq.ParquetFile(path).metadata
        out.append(
            {
                "file": path.relative_to(build).as_posix(),
                "bytes": path.stat().st_size,
                "rows": meta.num_rows,
                "row_groups": meta.num_row_groups,
            }
        )
    return out


def _markdown(rows: list[dict[str, Any]], keys: list[str]) -> str:
    lines = ["| " + " | ".join(keys) + " |", "|" + " --- |" * len(keys)]
    lines += ["| " + " | ".join(str(r[k]) for k in keys) + " |" for r in rows]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """Run the benchmark; with ``--one``, run one query and print its result as JSON."""
    parser = argparse.ArgumentParser(description="Time the store's agreed queries on its build.")
    parser.add_argument("root", type=Path, help="the store root")
    parser.add_argument("--seed", type=int, default=0, help="seed of the accession samples")
    parser.add_argument("--sizes", default="1,1000,100000", help="batch sizes of `get`")
    parser.add_argument("--out", type=Path, help="also write the results here, as JSON")
    parser.add_argument("--one", help=argparse.SUPPRESS)
    parser.add_argument("--samples", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.one:
        samples = json.loads(args.samples.read_text())
        start = time.perf_counter()
        rows = run_query(args.one, args.root, samples, Path.cwd())
        wall = time.perf_counter() - start
        print(json.dumps({"rows": rows, "wall_s": round(wall, 4), **_own_usage()}))
        return 0

    sizes = [int(n) for n in args.sizes.split(",")]
    build = build_dir(args.root)
    with tempfile.TemporaryDirectory(prefix="labdata-bench-") as name:
        tmp = Path(name)
        samples = sample(build, sizes, args.seed, tmp)
        samples_file = tmp / "samples.json"
        samples_file.write_text(json.dumps(samples))
        names = query_names(sizes, len(samples["series"]))
        queries = []
        for query in names:
            queries.append(measure(query, args.root, samples_file, tmp))
            print(json.dumps(queries[-1]), file=sys.stderr, flush=True)
    tables = table_stats(build)
    result = {"build": build.name, "seed": args.seed, "queries": queries, "tables": tables}
    print(f"build {build.name}, seed {args.seed}\n")
    print(_markdown(queries, ["query", "rows", "wall_s", "cpu_s", "peak_rss_mib"]) + "\n")
    print(_markdown(tables, ["file", "bytes", "rows", "row_groups"]))
    if args.out:
        args.out.write_text(json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
