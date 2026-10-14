"""Tabulate the ``!Sample_relation`` lines of GEO family SOFT files, by submission year.

For every Series touched by ``gsm.jsonl`` (from ``scripts/research/gsm_srx_join.py``) it
downloads the family SOFT from the GEO FTP site over HTTPS, then for every sample counts the
relation kinds (``SRA``, ``BioSample``, ...), how many SRX and SAMN accessions each carries,
and the URL shapes. It also checks the family file against the per-GSM record the join used.
Prints a JSON summary; the downloaded files are not kept.

Run::

    pixi run python scripts/research/field_sources/soft_relations.py --gsm-jsonl gsm.jsonl

or, for named Series only::

    pixi run python scripts/research/field_sources/soft_relations.py --series GSE20301
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import time
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

FTP = "https://ftp.ncbi.nlm.nih.gov/geo/series/{stub}/{gse}/soft/{gse}_family.soft.gz"
MAX_BYTES = 300_000_000


def fetch(url: str) -> bytes | None:
    """GET ``url``, retrying; None when the file is missing or larger than MAX_BYTES."""
    for attempt in range(4):
        time.sleep(0.4)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "labdata-research"})
            with urllib.request.urlopen(req, timeout=600) as resp:
                if int(resp.headers.get("Content-Length") or 0) > MAX_BYTES:
                    return None
                return resp.read()
        except OSError as exc:
            if "404" in str(exc):
                return None
            time.sleep(3.0 * (attempt + 1))
    return None


def samples(text: str) -> dict[str, dict[str, list[str]]]:
    """Family SOFT text -> GSM -> key -> values, skipping data tables."""
    out: dict[str, dict[str, list[str]]] = {}
    cur: dict[str, list[str]] | None = None
    in_table = False
    for line in text.splitlines():
        if line.endswith("_table_begin"):
            in_table = True
        elif line.endswith("_table_end"):
            in_table = False
        elif in_table:
            continue
        elif line.startswith("^"):
            kind, _, acc = line[1:].partition(" = ")
            cur = out.setdefault(acc.strip(), {}) if kind.strip() == "SAMPLE" else None
        elif cur is not None and line.startswith("!Sample_"):
            key, _, value = line[1:].partition(" = ")
            cur.setdefault(key.strip(), []).append(value.strip())
    return out


def shape(value: str) -> str:
    """Reduce a relation value to its shape: kind, URL host and path, accession pattern."""
    value = re.sub(r"\b(SRX|ERX|DRX)\d+", r"\1n", value)
    value = re.sub(r"\bSAM[NED][A-Z]?\d+", "SAMn", value)
    value = re.sub(r"\bGS[EM]\d+", "GSxn", value)
    return re.sub(r"\d+", "n", value)


def main() -> None:
    """Fetch, tabulate and print."""
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--gsm-jsonl", type=Path)
    ap.add_argument("--series", action="append", default=[])
    args = ap.parse_args()
    text = args.gsm_jsonl.read_text() if args.gsm_jsonl else ""
    rows = [json.loads(line) for line in text.splitlines()]
    wanted = {s for r in rows for s in r["series"]} | set(args.series)
    series = sorted(wanted, key=lambda s: int(s[3:]))
    notable: list[str] = []

    by_year: dict[str, Counter[str]] = defaultdict(Counter)
    shapes: Counter[str] = Counter()
    skipped: list[str] = []
    family: dict[str, dict[str, list[str]]] = {}
    for gse in series:
        num = gse[3:]
        stub = f"GSE{num[:-3]}nnn" if len(num) > 3 else "GSEnnn"
        raw = fetch(FTP.format(stub=stub, gse=gse))
        if raw is None:
            skipped.append(gse)
            continue
        for gsm, keys in samples(gzip.decompress(raw).decode("utf-8", "replace")).items():
            family.setdefault(gsm, keys)
    for gsm, keys in family.items():
        year = (keys.get("Sample_submission_date") or ["? ????"])[0][-4:]
        rels = keys.get("Sample_relation", [])
        sra = [r for r in rels if r.startswith("SRA:")]
        bios = [r for r in rels if r.startswith("BioSample:")]
        srx = {m for r in sra for m in re.findall(r"\b[SED]RX\d+", r)}
        sam = {m for r in bios for m in re.findall(r"\bSAM[NED][A-Z]?\d+", r)}
        c = by_year[year]
        c["samples"] += 1
        c[f"type_{(keys.get('Sample_type') or ['?'])[0]}"] += 1
        c["has_sra_line"] += bool(sra)
        c["has_biosample_line"] += bool(bios)
        c["one_srx"] += len(srx) == 1
        c["multi_srx"] += len(srx) > 1
        stype = (keys.get("Sample_type") or ["?"])[0]
        if len(srx) > 1 or (stype == "SRA" and not srx):
            notable.append(f"{gsm} {year} type={stype} srx={sorted(srx)}")
        c["one_samn"] += len(sam) == 1
        c["multi_sra_lines"] += len(sra) > 1
        c["sra_line_without_srx"] += bool(sra) and not srx
        for r in rels:
            shapes[shape(r)] += 1

    agree = Counter()
    for r in rows:
        keys = family.get(r["gsm"])
        if keys is None:
            agree["gsm_not_in_family_files"] += 1
            continue
        rels = keys.get("Sample_relation", [])
        srx = sorted({m for v in rels for m in re.findall(r"\b[SED]RX\d+", v)})
        agree["srx_same_as_gsm_record"] += srx == r["srx_soft"]
    summary: dict[str, Any] = {
        "series": len(series),
        "series_skipped": skipped,
        "samples": len(family),
        "by_submission_year": {y: dict(c) for y, c in sorted(by_year.items())},
        "relation_shapes": shapes.most_common(25),
        "family_vs_gsm_record": dict(agree),
        "multi_srx_or_sra_type_without_srx": notable[:200],
    }
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
