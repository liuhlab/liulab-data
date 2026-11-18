"""Compare the bulk GEO Series esummary dump with live family SOFT and live esummary.

Two steps, because the dump and the network may sit on different machines:

``extract`` streams the gzip JSONL dump once, counts its records, and writes the records of
the wanted Series plus whole-dump field tallies to ``--out``.

``compare`` fetches live family SOFT and live ``esummary db=gds`` for the same Series and
reports, per field, whether the dump is the same as SOFT, truncated, missing or different.

Run::

    python3 scripts/research/field_sources/geo_esummary_check.py extract \
        --dump "$BULK/geo-series/2026-10-14/gds_gse_esummary.jsonl.gz" --series series.txt --out out/
    pixi run python scripts/research/field_sources/geo_esummary_check.py compare \
        --series series.txt --out out/
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
FTP = "https://ftp.ncbi.nlm.nih.gov/geo/series/{stub}/{gse}/soft/{gse}_family.soft.gz"


def extract(dump: Path, wanted: set[str], out: Path) -> None:
    """Stream the dump; keep the wanted records and tally every field."""
    tally: Counter[str] = Counter()
    kept: list[dict[str, Any]] = []
    accessions: set[str] = set()
    with gzip.open(dump, "rt") as fh:
        for line in fh:
            rec = json.loads(line)
            acc = rec.get("accession", "")
            tally["records"] += 1
            accessions.add(acc)
            tally[f"entrytype_{rec.get('entrytype')}"] += 1
            for key in ("summary", "pubmedids", "relations", "extrelations", "bioproject"):
                tally[f"nonempty_{key}"] += bool(rec.get(key))
            tally["samples_list_equals_n_samples"] += len(rec.get("samples") or []) == rec.get(
                "n_samples"
            )
            tally["summary_ends_with_ellipsis"] += str(rec.get("summary", "")).endswith("...")
            if acc in wanted:
                kept.append(rec)
    tally["distinct_accessions"] = len(accessions)
    out.mkdir(parents=True, exist_ok=True)
    (out / "geo_dump_rows.jsonl").write_text("".join(json.dumps(r) + "\n" for r in kept))
    (out / "geo_dump_tally.json").write_text(json.dumps(dict(tally), indent=1))
    print(json.dumps(dict(tally), indent=1), f"kept {len(kept)} of {len(wanted)}")


def get(url: str) -> bytes:
    """GET with a polite pause and retries."""
    for attempt in range(5):
        time.sleep(0.4)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "labdata-research"})
            with urllib.request.urlopen(req, timeout=600) as resp:
                return resp.read()
        except OSError:
            time.sleep(3.0 * (attempt + 1))
    raise RuntimeError(url)


def series_soft(gse: str) -> tuple[dict[str, list[str]], dict[str, str], list[str]]:
    """Family SOFT -> series keys, GSM -> title, and the samples' organisms."""
    num = gse[3:]
    stub = f"GSE{num[:-3]}nnn" if len(num) > 3 else "GSEnnn"
    text = gzip.decompress(get(FTP.format(stub=stub, gse=gse))).decode("utf-8", "replace")
    series: dict[str, list[str]] = {}
    titles: dict[str, str] = {}
    organisms: list[str] = []
    kind, acc, in_table = "", "", False
    for line in text.splitlines():
        if line.endswith("_table_begin"):
            in_table = True
        elif line.endswith("_table_end"):
            in_table = False
        elif in_table:
            continue
        elif line.startswith("^"):
            kind, _, acc = (p.strip() for p in line[1:].partition(" = "))
        elif line.startswith("!"):
            key, _, value = (p.strip() for p in line[1:].partition(" = "))
            if kind == "SERIES" and acc == gse:
                series.setdefault(key, []).append(value)
            elif kind == "SAMPLE" and key == "Sample_title":
                titles[acc] = value
            elif kind == "SAMPLE" and key.startswith("Sample_organism_ch"):
                organisms.append(value)
    return series, titles, organisms


def norm(text: str) -> str:
    """Collapse whitespace."""
    return " ".join(text.split())


def judge(dump_value: Any, soft_value: Any) -> str:
    """Classify one field: same, truncated, missing or different."""
    if dump_value in (None, "", [], set()):
        return "missing" if soft_value not in (None, "", [], set()) else "same"
    if isinstance(dump_value, str) and isinstance(soft_value, str):
        d, s = norm(dump_value), norm(soft_value)
        if d == s:
            return "same"
        if s.startswith(d.rstrip(". ")) or (d.endswith("...") and s.startswith(d[:-3].strip())):
            return "truncated"
        return "different"
    return "same" if dump_value == soft_value else "different"


def public_date(series: dict[str, list[str]]) -> str:
    """``Public on Jan 19 2015`` -> ``2015/01/19``."""
    status = (series.get("Series_status") or [""])[0].replace("Public on ", "")
    try:
        return datetime.strptime(status, "%b %d %Y").strftime("%Y/%m/%d")
    except ValueError:
        return ""


def relations(values: list[str], label: str) -> set[str]:
    """GSE accessions in the ``Series_relation`` lines that start with ``label``."""
    return {m for v in values if v.startswith(label) for m in re.findall(r"GSE\d+", v)}


def compare(wanted: list[str], out: Path) -> None:
    """Fetch live SOFT and esummary; judge each dump field."""
    dump = {
        r["accession"]: r
        for r in map(json.loads, (out / "geo_dump_rows.jsonl").read_text().splitlines())
    }
    uids = [str(200_000_000 + int(g[3:])) for g in wanted]
    live: dict[str, Any] = {}
    for i in range(0, len(uids), 100):
        query = urllib.parse.urlencode(
            {"db": "gds", "id": ",".join(uids[i : i + 100]), "retmode": "json"}
        )
        result = json.loads(get(f"{EUTILS}esummary.fcgi?{query}"))["result"]
        live.update({result[u]["accession"]: result[u] for u in result.get("uids", [])})
    count = json.loads(
        get(f"{EUTILS}esearch.fcgi?db=gds&term=gse%5BETYP%5D&retmode=json&retmax=0")
    )["esearchresult"]["count"]

    verdicts: dict[str, Counter[str]] = {}
    examples: dict[str, list[str]] = {}
    drift: Counter[str] = Counter()
    for gse in wanted:
        d = dump.get(gse)
        if d is None:
            verdicts.setdefault("record", Counter())["missing"] += 1
            continue
        series, titles, organisms = series_soft(gse)
        rel = series.get("Series_relation", [])
        dump_rel = {str(r.get("targetobject", "")) for r in d.get("relations") or []}
        checks = {
            "title": (d["title"], " ".join(series.get("Series_title", []))),
            "summary": (d["summary"], " ".join(series.get("Series_summary", []))),
            "organism": (set(filter(None, d["taxon"].split("; "))), set(organisms)),
            "gpl": (
                {f"GPL{x}" for x in d["gpl"].split(";") if x},
                set(series.get("Series_platform_id", [])),
            ),
            "type": (set(d["gdstype"].split("; ")), set(series.get("Series_type", []))),
            "n_samples": (d["n_samples"], len(series.get("Series_sample_id", []))),
            "pubmed_ids": (set(d["pubmedids"]), set(series.get("Series_pubmed_id", []))),
            "bioproject": (
                set(filter(None, d["bioproject"].split(";"))),
                {m for v in rel for m in re.findall(r"PRJ[A-Z]+\d+", v)},
            ),
            "superseries_subseries": (
                dump_rel,
                relations(rel, "SubSeries of") | relations(rel, "SuperSeries of"),
            ),
            "public_date": (d["pdat"], public_date(series)),
            "gsm_list": (
                {s["accession"] for s in d["samples"]},
                set(series.get("Series_sample_id", [])),
            ),
            "gsm_titles": (
                {s["accession"]: s["title"] for s in d["samples"]},
                {g: titles.get(g, "") for g in series.get("Series_sample_id", [])},
            ),
        }
        for field, (dv, sv) in checks.items():
            verdict = judge(dv, sv)
            verdicts.setdefault(field, Counter())[verdict] += 1
            if verdict not in ("same",) and len(examples.setdefault(field, [])) < 4:
                examples[field].append(f"{gse}: dump={str(dv)[:120]!r} soft={str(sv)[:120]!r}")
        lv = live.get(gse, {})
        for key, value in d.items():
            drift[f"{key}_{'same' if lv.get(key) == value else 'changed'}"] += 1
    summary = {
        "live_gse_count": count,
        "series": len(wanted),
        "verdicts": {k: dict(v) for k, v in verdicts.items()},
        "examples": examples,
        "dump_vs_live_esummary": {k: v for k, v in sorted(drift.items()) if "changed" in k},
    }
    (out / "geo_dump_compare.json").write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary, indent=1))


def main() -> None:
    """Dispatch the two steps."""
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("step", choices=["extract", "compare"])
    ap.add_argument("--dump", type=Path)
    ap.add_argument("--series", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    wanted = sorted(set(args.series.read_text().split()), key=lambda g: int(g[3:]))
    if args.step == "extract":
        if args.dump is None:
            ap.error("extract needs --dump")
        extract(args.dump, set(wanted), args.out)
    else:
        compare(wanted, args.out)


if __name__ == "__main__":
    main()
