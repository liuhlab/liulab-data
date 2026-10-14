"""Compare the ``SRA_Accessions.tab`` extract with the live GSM and run values.

Reads ``accessions_rows.tsv`` (from ``accessions_check.sh``), ``gsm.jsonl`` (from
``gsm_srx_join.py``) and ``live_runs.tsv`` (from ``live_reference.py``). For each sampled GSM
it finds the SRX through three bulk routes and compares them with the SOFT link. For each live
run it compares spots, bases and the release date. Prints a JSON summary.

Run::

    python3 scripts/research/field_sources/accessions_compare.py \
        --rows out/accessions_rows.tsv --gsm-jsonl live/gsm.jsonl --live live/live_runs.tsv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


def main() -> None:
    """Compare and print the summary."""
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--rows", type=Path, required=True)
    ap.add_argument("--gsm-jsonl", type=Path, required=True)
    ap.add_argument("--live", type=Path, required=True)
    args = ap.parse_args()

    with args.rows.open() as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    by_acc = {r["Accession"]: r for r in rows}
    exp_alias: dict[str, set[str]] = {}
    run_alias: dict[str, set[str]] = {}
    sample_alias: dict[str, set[str]] = {}
    for r in rows:
        m = re.match(r"(GSM\d+)(.*)$", r["Alias"])
        if not m:
            continue
        gsm, rest = m.groups()
        if r["Type"] == "EXPERIMENT" and (rest == "" or rest.startswith(": ")):
            exp_alias.setdefault(gsm, set()).add(r["Accession"])
        elif r["Type"] == "RUN" and re.fullmatch(r"_\d+", rest):
            run_alias.setdefault(gsm, set()).add(r["Experiment"])
        elif r["Type"] == "SAMPLE" and rest == "":
            sample_alias.setdefault(gsm, set()).add(r["Accession"])
    exp_of_sample: dict[str, set[str]] = {}
    for r in rows:
        if r["Type"] == "RUN":
            exp_of_sample.setdefault(r["Sample"], set()).add(r["Experiment"])

    gsm_rows = [json.loads(line) for line in args.gsm_jsonl.read_text().splitlines()]
    routes: Counter[str] = Counter()
    misses: dict[str, list[str]] = {}
    for g in gsm_rows:
        soft = set(g["srx_soft"])
        found = {
            "experiment_alias": exp_alias.get(g["gsm"], set()),
            "run_alias": run_alias.get(g["gsm"], set()),
            "sample_alias": {
                x for s in sample_alias.get(g["gsm"], set()) for x in exp_of_sample.get(s, set())
            },
        }
        found["any_alias"] = set().union(*found.values())
        for route, srx in found.items():
            if soft:
                routes[f"{route}_equals_soft"] += srx == soft
            else:
                routes[f"{route}_finds_srx_soft_lacks"] += bool(srx)
            if soft and srx != soft:
                misses.setdefault(route, []).append(f"{g['year']} {g['gsm']} {sorted(srx)}")
        for srx in soft:
            status = by_acc.get(srx, {}).get("Status", "absent")
            routes[f"soft_srx_status_{status}"] += 1

    with args.live.open() as fh:
        live = list(csv.DictReader(fh, delimiter="\t"))
    runs: Counter[str] = Counter()
    run_diffs: list[str] = []
    for lr in live:
        br: dict[str, Any] | None = by_acc.get(lr["srr"])
        if br is None:
            runs["absent"] += 1
            continue
        runs[f"status_{br['Status']}_{br['Visibility']}"] += 1
        for lf, bf in (("spots", "Spots"), ("bases", "Bases")):
            same = (lr[lf] or "-") == br[bf]
            runs[f"{lf}_equal"] += same
            if not same:
                run_diffs.append(f"{lr['srr']} {lf}: live={lr[lf]!r} bulk={br[bf]!r}")
        runs["published_day_equal"] += lr["published"][:10] == br["Published"][:10]
        runs["biosample_equal"] += lr["biosample"] == br["BioSample"]
        runs["bioproject_equal"] += lr["bioproject"] == br["BioProject"]
    print(
        json.dumps(
            {
                "gsms": len(gsm_rows),
                "gsms_with_soft_srx": sum(bool(g["srx_soft"]) for g in gsm_rows),
                "routes": dict(routes),
                "misses": misses,
                "runs": dict(runs),
                "run_diffs": run_diffs,
            },
            indent=1,
        )
    )


if __name__ == "__main__":
    main()
