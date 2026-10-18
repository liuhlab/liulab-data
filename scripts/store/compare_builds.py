"""Compare two builds cell for cell: every table, every row, every column.

Usage::

    pixi run python scripts/store/compare_builds.py OLD_BUILD NEW_BUILD

Both arguments are build folders, such as ``<store>/builds/<build-id>``. Rows are matched by
position, so both builds must sort each table the same way. A column typed as a date or a
timestamp on either side is compared at day level, so a text date matches its typed form.
Parquet key-value metadata is compared too, except the Arrow schema that the writer adds.

It prints one line per difference, naming the file, the column, the count of rows that
differ and the first of them, and exits 1. With no difference it exits 0. Only reads.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

#: Characters of a cell shown in a difference.
SHOWN = 120


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _is_date(kind: pa.DataType) -> bool:
    return pa.types.is_date(kind) or pa.types.is_timestamp(kind)


def _term(name: str, kind: pa.DataType, as_date: bool) -> str:
    """Return the value compared for one column; a date column compares at day level."""
    if not as_date:
        return _ident(name)
    if _is_date(kind):
        return f"CAST({_ident(name)} AS DATE)"
    return f"CAST(CAST(nullif(CAST({_ident(name)} AS VARCHAR), '') AS TIMESTAMP) AS DATE)"


def _shown(before: object, after: object) -> str:
    """Return both cells for a report; long texts are cut around their first difference."""
    if (
        isinstance(before, str)
        and isinstance(after, str)
        and max(map(len, (before, after))) > SHOWN
    ):
        at = next(
            (i for i, (x, y) in enumerate(zip(before, after, strict=False)) if x != y),
            min(len(before), len(after)),
        )
        start = max(0, at - SHOWN // 2)
        cut = [repr(t[start : start + SHOWN]) for t in (before, after)]
        return f"at character {at}: ...{cut[0]}... became ...{cut[1]}..."
    return f"{before!r} became {after!r}"


def compare_file(con: duckdb.DuckDBPyConnection, name: str, old: Path, new: Path) -> list[str]:
    """Return how one table file differs between the builds; empty if it does not."""
    old_schema, new_schema = pq.read_schema(old), pq.read_schema(new)
    out: list[str] = []
    meta = [
        {k: v for k, v in (s.metadata or {}).items() if not k.startswith(b"ARROW:")}
        for s in (old_schema, new_schema)
    ]
    if meta[0] != meta[1]:
        out.append(f"{name}: key-value metadata {meta[0]} became {meta[1]}")
    if old_schema.names != new_schema.names:
        out.append(f"{name}: columns {old_schema.names} became {new_schema.names}")
        return out
    columns = old_schema.names
    dated = [
        _is_date(old_schema.field(c).type) or _is_date(new_schema.field(c).type) for c in columns
    ]
    side = {
        prefix: ", ".join(
            f"{_term(c, schema.field(c).type, d)} AS {prefix}{i}"
            for i, (c, d) in enumerate(zip(columns, dated, strict=True))
        )
        for prefix, schema in (("a", old_schema), ("b", new_schema))
    }
    joined = (
        f"(SELECT file_row_number AS rn, {side['a']} "
        f"FROM read_parquet({_quote(str(old))}, file_row_number = true)) "
        f"POSITIONAL JOIN (SELECT {side['b']} FROM read_parquet({_quote(str(new))}))"
    )
    differs = [f"a{i} IS DISTINCT FROM b{i}" for i in range(len(columns))]
    counts = con.execute(
        "SELECT "
        + ", ".join(f"count(*) FILTER (WHERE {d}), min(rn) FILTER (WHERE {d})" for d in differs)
        + f" FROM {joined}"
    ).fetchone()
    assert counts is not None
    old_rows = pq.ParquetFile(old).metadata.num_rows
    new_rows = pq.ParquetFile(new).metadata.num_rows
    if old_rows != new_rows:
        out.append(f"{name}: {old_rows} rows became {new_rows}")
    for i, column in enumerate(columns):
        count, first = counts[2 * i], counts[2 * i + 1]
        if not count:
            continue
        where = f"rn = {first}" if first is not None else "rn IS NULL"
        key, before, after = con.execute(
            f"SELECT a0, a{i}, b{i} FROM {joined} WHERE {where} LIMIT 1"
        ).fetchone() or (None, None, None)
        rows = "1 row differs" if count == 1 else f"{count} rows differ"
        out.append(
            f"{name}: column {column}: {rows}; first at row {first} "
            f"({columns[0]} {key}): {_shown(before, after)}"
        )
    return out


def compare(old: Path, new: Path) -> list[str]:
    """Return every difference between two build folders' tables; empty if none."""
    names = sorted({p.relative_to(b).as_posix() for b in (old, new) for p in b.rglob("*.parquet")})
    out: list[str] = []
    with tempfile.TemporaryDirectory(prefix="labdata-compare-") as tmp:
        con = duckdb.connect(config={"temp_directory": tmp})
        for name in names:
            missing = [
                label for label, b in (("old", old), ("new", new)) if not (b / name).is_file()
            ]
            if missing:
                out.append(f"{name}: missing from the {missing[0]} build")
                continue
            out += compare_file(con, name, old / name, new / name)
            print(f"compared {name}", file=sys.stderr, flush=True)
        con.close()
    return out


def main(argv: list[str] | None = None) -> int:
    """Compare two builds; print each difference and return 1 if there is any."""
    parser = argparse.ArgumentParser(description="Compare two builds cell for cell.")
    parser.add_argument("old", type=Path, help="the old build folder")
    parser.add_argument("new", type=Path, help="the new build folder")
    args = parser.parse_args(argv)
    for build in (args.old, args.new):
        if not build.is_dir():
            parser.error(f"{build} is not a folder")
    differences = compare(args.old, args.new)
    for line in differences:
        print(line)
    if differences:
        return 1
    print(f"the builds match: {args.old.name} and {args.new.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
