"""Compare bulk ``biosample_set.xml.gz`` and ``bioproject.xml`` records with live efetch replies.

Streams each bulk file once, keeps the records whose accession appears in the live reference
(``live_biosamples.json`` and ``live_bioprojects.json`` from ``live_reference.py``), writes
them to ``--out`` and prints which fields agree.

Run::

    python3 scripts/research/field_sources/xml_check.py \
        --bulk-root "$BULK" --date 2026-10-14 --live live/ --out out/
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from xml.etree import ElementTree


def records(path: Path, start: bytes, end: bytes) -> Iterator[bytes]:
    """Yield each ``start``...``end`` block of a large XML file, streamed line by line."""
    cmd = ["zcat", str(path)] if path.suffix == ".gz" else ["cat", str(path)]
    with subprocess.Popen(cmd, stdout=subprocess.PIPE) as proc:
        assert proc.stdout is not None
        buf: list[bytes] | None = None
        for line in proc.stdout:
            stripped = line.lstrip()
            if buf is None:
                if stripped.startswith(start):
                    buf = [line]
                continue
            buf.append(line)
            if stripped.startswith(end):
                yield b"".join(buf)
                buf = None


def norm(value: Any) -> str:
    """Case- and space-insensitive text."""
    return " ".join(str(value or "").lower().split())


def check_biosamples(path: Path, live: dict[str, Any], out: Path) -> dict[str, Any]:
    """Extract and compare BioSample records."""
    wanted = set(live)
    acc_re = re.compile(rb'accession="(SAM[A-Z]+\d+)"')
    kept: dict[str, Any] = {}
    with (out / "biosample_records.xml").open("wb") as fh:
        for rec in records(path, b"<BioSample ", b"</BioSample>"):
            m = acc_re.search(rec, 0, 400)
            if m and m.group(1).decode() in wanted:
                fh.write(rec)
                bs = ElementTree.fromstring(rec)
                org = bs.find("Description/Organism")
                kept[m.group(1).decode()] = {
                    "title": " ".join((bs.findtext("Description/Title") or "").split()),
                    "taxid": org.get("taxonomy_id") if org is not None else None,
                    "package": bs.findtext("Package"),
                    "last_update": bs.get("last_update"),
                    "attributes": [
                        [a.get("attribute_name"), a.get("harmonized_name"), (a.text or "").strip()]
                        for a in bs.iter("Attribute")
                    ],
                }
                if len(kept) == len(wanted):
                    break
    tally: Counter[str] = Counter()
    diffs: list[str] = []
    for acc, bulk in kept.items():
        for field in ("title", "taxid", "package", "last_update", "attributes"):
            same = bulk[field] == live[acc][field]
            tally[f"{field}_equal"] += same
            if not same:
                diffs.append(f"{acc} {field}: live={live[acc][field]!r} bulk={bulk[field]!r}")
    return {
        "live": len(live),
        "found": len(kept),
        "missing": sorted(wanted - set(kept)),
        "tally": dict(tally),
        "diffs": diffs[:20],
    }


def check_bioprojects(path: Path, live: dict[str, Any], out: Path) -> dict[str, Any]:
    """Extract and compare BioProject records."""
    wanted = set(live)
    acc_re = re.compile(rb'<ArchiveID accession="(PRJ[A-Z]+\d+)"')
    kept: dict[str, Any] = {}
    with (out / "bioproject_records.xml").open("wb") as fh:
        for rec in records(path, b"<Package>", b"</Package>"):
            m = acc_re.search(rec)
            if m and m.group(1).decode() in wanted:
                fh.write(rec)
                doc = ElementTree.fromstring(rec)
                sub = doc.find(".//Submission")
                kept[m.group(1).decode()] = {
                    "title": doc.findtext(".//ProjectDescr/Title"),
                    "description": " ".join(
                        (doc.findtext(".//ProjectDescr/Description") or "").split()
                    ),
                    "publications": sorted(p.get("id") or "" for p in doc.iter("Publication")),
                    "data_types": sorted(d.text or "" for d in doc.iter("DataType")),
                    "submitted": sub.get("submitted") if sub is not None else None,
                    "last_update": sub.get("last_update") if sub is not None else None,
                }
                if len(kept) == len(wanted):
                    break
    tally: Counter[str] = Counter()
    diffs: list[str] = []
    for acc, bulk in kept.items():
        for field, value in bulk.items():
            same = value == live[acc][field]
            tally[f"{field}_equal"] += same
            if not same:
                diffs.append(f"{acc} {field}: live={live[acc][field]!r} bulk={value!r}")
    return {
        "live": len(live),
        "found": len(kept),
        "missing": sorted(wanted - set(kept)),
        "tally": dict(tally),
        "diffs": diffs[:20],
    }


def main() -> None:
    """Run both checks and write the summary."""
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--bulk-root", type=Path, required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--live", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--only", choices=["biosample", "bioproject"])
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {}
    if args.only in (None, "bioproject"):
        summary["bioproject"] = check_bioprojects(
            args.bulk_root / "bioproject" / args.date / "bioproject.xml",
            json.loads((args.live / "live_bioprojects.json").read_text()),
            args.out,
        )
    if args.only in (None, "biosample"):
        summary["biosample"] = check_biosamples(
            args.bulk_root / "biosample" / args.date / "biosample_set.xml.gz",
            json.loads((args.live / "live_biosamples.json").read_text()),
            args.out,
        )
    name = f"xml_summary_{args.only or 'all'}.json"
    (args.out / name).write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
