"""Measure how lossy the GEO sample (GSM) to SRA experiment (SRX) join is.

Draws a random sample of GEO samples on high-throughput-sequencing platforms, a fixed number
per publication year, and checks each one against SRA and BioSample over E-utilities. It
writes one JSON line per sample and prints a summary. Network only; nothing is kept beyond
the output file.

Credentials come from labdata's own resolver (``NCBI_EMAIL``/``NCBI_API_KEY``, then the
labdata config file). Without them the script runs keyless at the keyless rate limit.

Run::

    pixi run python scripts/research/gsm_srx_join.py --per-year 4 --seed 23 --out gsm.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
import re
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from labdata.exceptions import CredentialsError
from labdata.ncbi import load_credentials

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
GSM_SOFT = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ=self&form=text&view=brief&acc="
BATCH = 100


class Ncbi:
    """A rate-limited GET client for E-utilities and the GEO accession page."""

    def __init__(self) -> None:
        try:
            creds = load_credentials(allow_prompt=False)
            self.auth = {
                "email": creds.email,
                **({"api_key": creds.api_key} if creds.api_key else {}),
            }
        except CredentialsError:
            self.auth = {}
        self.gap = 0.12 if "api_key" in self.auth else 0.4
        self.last = 0.0

    def get(self, url: str) -> str:
        """Fetch ``url`` as text, retrying transient failures."""
        for attempt in range(5):
            time.sleep(max(0.0, self.gap - (time.time() - self.last)))
            self.last = time.time()
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "labdata-research"})
                with urllib.request.urlopen(req, timeout=300) as resp:
                    text = resp.read().decode("utf-8", "replace")
                if "API rate limit exceeded" not in text[:300]:
                    return text
            except OSError:
                pass
            time.sleep(2.0 * (attempt + 1))
        raise RuntimeError(f"gave up on {url[:200]}")

    def eu(self, tool: str, **params: str) -> str:
        """Call one E-utility with the credentials attached."""
        query = urllib.parse.urlencode({**params, **self.auth, "tool": "labdata-research"})
        return self.get(f"{EUTILS}{tool}.fcgi?{query}")


def sample_gsms(ncbi: Ncbi, years: range, per_year: int, seed: int) -> list[tuple[int, str]]:
    """Pick ``per_year`` random sequencing-platform GSM uids from each publication year."""
    rng = random.Random(seed)
    picked: list[tuple[int, str]] = []
    for year in years:
        term = f'gsm[ETYP] AND "high throughput sequencing"[PTYP] AND {year}[PDAT]'
        count = int(
            json.loads(ncbi.eu("esearch", db="gds", term=term, retmode="json", retmax="0"))[
                "esearchresult"
            ]["count"]
        )
        for offset in sorted(rng.sample(range(count), min(per_year, count))):
            reply = ncbi.eu(
                "esearch", db="gds", term=term, retmode="json", retstart=str(offset), retmax="1"
            )
            picked.extend((year, uid) for uid in json.loads(reply)["esearchresult"]["idlist"])
    return picked


def parse_gsm_soft(text: str) -> dict[str, list[str]]:
    """GSM SOFT text -> key -> values, for the first SAMPLE block."""
    out: dict[str, list[str]] = {}
    for line in text.splitlines():
        if line.startswith("!Sample_"):
            key, _, value = line[1:].partition(" = ")
            out.setdefault(key.strip(), []).append(value.strip())
    return out


def split_characteristic(raw: str) -> tuple[str, str]:
    """``"key: value"`` -> (key, value); a bare value gets an empty key."""
    key, sep, value = raw.partition(": ")
    return (key.strip(), value.strip()) if sep else ("", raw.strip())


def sra_packages(ncbi: Ncbi, ids: list[str]) -> tuple[dict[str, dict[str, Any]], int]:
    """Fetch SRA packages by SRX accession, refetching any that efetch silently drops."""
    found: dict[str, dict[str, Any]] = {}
    dropped = 0
    todo = list(ids)
    for _round in range(3):
        for i in range(0, len(todo), BATCH):
            chunk = todo[i : i + BATCH]
            root = ElementTree.fromstring(ncbi.eu("efetch", db="sra", id=",".join(chunk)))
            for pkg in root.iter("EXPERIMENT_PACKAGE"):
                facts = package_facts(pkg)
                found[facts["srx"]] = facts
        missing = [x for x in todo if x not in found]
        if _round == 0:
            dropped = len(missing)
        if not missing:
            break
        todo = missing
    return found, dropped


def package_facts(pkg: ElementTree.Element) -> dict[str, Any]:
    """Extract the fields of one EXPERIMENT_PACKAGE that this measurement compares."""
    exp = pkg.find("EXPERIMENT")
    sample = pkg.find("SAMPLE")
    lib = exp.find("DESIGN/LIBRARY_DESCRIPTOR") if exp is not None else None

    def ext(node: ElementTree.Element | None, ns: str) -> str | None:
        if node is None:
            return None
        for ident in node.iter("EXTERNAL_ID"):
            if ident.get("namespace") == ns:
                return (ident.text or "").strip()
        return None

    def txt(node: ElementTree.Element | None, path: str) -> str:
        return " ".join(((node.findtext(path) if node is not None else "") or "").split())

    layout = lib.find("LIBRARY_LAYOUT") if lib is not None else None
    return {
        "srx": exp.get("accession") if exp is not None else "",
        "alias": exp.get("alias") if exp is not None else None,
        "center": exp.get("center_name") if exp is not None else None,
        "sample_geo": ext(sample, "GEO"),
        "biosample": ext(sample, "BioSample"),
        "taxid": txt(sample, "SAMPLE_NAME/TAXON_ID"),
        "strategy": txt(lib, "LIBRARY_STRATEGY"),
        "source": txt(lib, "LIBRARY_SOURCE"),
        "selection": txt(lib, "LIBRARY_SELECTION"),
        "layout": layout[0].tag if layout is not None and len(layout) else None,
        "instrument": txt(exp, ".//INSTRUMENT_MODEL"),
        "runs": len(pkg.findall(".//RUN")),
        "attributes": [
            (txt(a, "TAG"), txt(a, "VALUE")) for a in pkg.findall("SAMPLE//SAMPLE_ATTRIBUTE")
        ],
    }


def biosamples(ncbi: Ncbi, ids: list[str]) -> dict[str, list[tuple[str, str]]]:
    """Fetch BioSamples as accession -> (attribute_name, value) pairs."""
    out: dict[str, list[tuple[str, str]]] = {}
    for i in range(0, len(ids), BATCH):
        reply = ncbi.eu("efetch", db="biosample", id=",".join(ids[i : i + BATCH]), rettype="full")
        for bs in ElementTree.fromstring(reply).iter("BioSample"):
            out[bs.get("accession") or ""] = [
                (a.get("attribute_name") or "", " ".join((a.text or "").split()))
                for a in bs.iter("Attribute")
            ]
    return out


def elink_srx(ncbi: Ncbi, gsm_uids: list[str]) -> dict[str, list[str]]:
    """GSM uid -> SRX accessions via elink gds->sra and esummary sra."""
    links: dict[str, list[str]] = {}
    for i in range(0, len(gsm_uids), BATCH):
        params = [("dbfrom", "gds"), ("db", "sra"), ("retmode", "json")]
        params += [("id", u) for u in gsm_uids[i : i + BATCH]]
        reply = json.loads(ncbi.get(f"{EUTILS}elink.fcgi?{urllib.parse.urlencode(params)}"))
        for ls in reply.get("linksets", []):
            dbs = ls.get("linksetdbs", [])
            links[ls["ids"][0]] = [x for d in dbs if d["linkname"] == "gds_sra" for x in d["links"]]
    sra_uids = sorted({u for v in links.values() for u in v})
    srx_of: dict[str, str] = {}
    for i in range(0, len(sra_uids), BATCH):
        reply = json.loads(
            ncbi.eu("esummary", db="sra", id=",".join(sra_uids[i : i + BATCH]), retmode="json")
        )
        for uid in reply["result"].get("uids", []):
            match = re.search(r'Experiment acc="(\w+)"', reply["result"][uid]["expxml"])
            if match:
                srx_of[uid] = match.group(1)
    return {g: sorted(srx_of.get(u, f"uid:{u}") for u in v) for g, v in links.items()}


def norm(value: str | None) -> str:
    """Case- and space-insensitive form for comparing controlled values."""
    return " ".join((value or "").lower().replace("_", " ").split())


def compare(gsm: dict[str, Any], sra: dict[str, dict[str, Any]], bs: dict[str, Any]) -> None:
    """Add the join checks for one GSM to its row, in place."""
    pkgs = [sra[x] for x in gsm["srx_soft"] if x in sra]
    gsm["srx_found"] = len(pkgs)
    gsm["runs"] = sum(p["runs"] for p in pkgs)
    gsm["alias_is_gsm"] = all(gsm["gsm"] in (p["alias"], p["sample_geo"]) for p in pkgs)
    gsm["center_geo"] = all(p["center"] == "GEO" for p in pkgs)
    gsm["biosample_agree"] = all(p["biosample"] == gsm["biosample_soft"] for p in pkgs)
    gsm["layout"] = sorted({p["layout"] or "" for p in pkgs})
    for field, soft_key in (
        ("strategy", "Sample_library_strategy"),
        ("source", "Sample_library_source"),
        ("selection", "Sample_library_selection"),
        ("instrument", "Sample_instrument_model"),
        ("taxid", "Sample_taxid_ch1"),
    ):
        geo_value = norm((gsm["soft"].get(soft_key) or [""])[0])
        gsm[f"{field}_agree"] = all(norm(p[field]) == geo_value for p in pkgs) if pkgs else None
        gsm[f"{field}_pairs"] = sorted({(geo_value, norm(p[field])) for p in pkgs})
    chars = [split_characteristic(c) for c in gsm["soft"].get("Sample_characteristics_ch1", [])]
    gsm["n_char"] = len(chars)
    for name, pairs in (
        ("biosample", bs.get(gsm["biosample_soft"] or "")),
        ("sra_sample", pkgs[0]["attributes"] if pkgs else None),
    ):
        if pairs is None:
            gsm[f"char_in_{name}"] = None
            continue
        have = {(k.lower(), v) for k, v in pairs}
        gsm[f"char_in_{name}"] = sum((k.lower(), v) in have for k, v in chars)
        gsm[f"char_values_in_{name}"] = sum(v in {pv for _k, pv in pairs} for _k, v in chars)
        gsm[f"extra_in_{name}"] = sorted(
            {k for k, _v in pairs} - {k for k, _v in chars} - {"source_name"}
        )
    gsm.pop("soft")


def main() -> None:
    """Sample, fetch, compare and summarise."""
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--first-year", type=int, default=2008)
    ap.add_argument("--last-year", type=int, default=2025)
    ap.add_argument("--per-year", type=int, default=4)
    ap.add_argument("--seed", type=int, default=23)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    ncbi = Ncbi()

    picked = sample_gsms(ncbi, range(args.first_year, args.last_year + 1), args.per_year, args.seed)
    rows: list[dict[str, Any]] = []
    for year, uid in picked:
        gsm = f"GSM{int(uid) - 300_000_000}"
        soft = parse_gsm_soft(ncbi.get(GSM_SOFT + gsm))
        rels = soft.get("Sample_relation", [])
        rows.append(
            {
                "gsm": gsm,
                "uid": uid,
                "year": year,
                "series": soft.get("Sample_series_id", []),
                "type": (soft.get("Sample_type") or [""])[0],
                "relations": sorted({r.split(":")[0] for r in rels}),
                "srx_soft": sorted({m for r in rels for m in re.findall(r"\b[SDE]RX\d+", r)}),
                "biosample_soft": next(
                    (m.group(0) for r in rels if (m := re.search(r"SAM[NED][A-Z]?\d+", r))), None
                ),
                "soft": soft,
            }
        )
    links = elink_srx(ncbi, [r["uid"] for r in rows])
    all_srx = sorted({x for r in rows for x in r["srx_soft"]})
    sra, dropped = sra_packages(ncbi, all_srx)
    bs_ids = sorted({r["biosample_soft"] for r in rows if r["biosample_soft"]})
    bs = biosamples(ncbi, bs_ids)
    for row in rows:
        row["srx_elink"] = links.get(row["uid"], [])
        compare(row, sra, bs)

    args.out.write_text("".join(json.dumps(r) + "\n" for r in rows))
    summarise(rows, dropped)


def summarise(rows: list[dict[str, Any]], dropped: int) -> None:
    """Print the counts the research note quotes."""
    n = len(rows)
    with_srx = [r for r in rows if r["srx_soft"]]
    print(f"GSMs sampled: {n}; years {min(r['year'] for r in rows)}-{max(r['year'] for r in rows)}")
    print(f"Series touched: {len({s for r in rows for s in r['series']})}")
    print("Sample_type:", dict(Counter(r["type"] for r in rows)))
    print("relation kinds:", dict(Counter(k for r in rows for k in r["relations"])))
    print(f"GSM with no SRX in SOFT: {n - len(with_srx)}")
    print(f"GSM with >1 SRX: {sum(len(r['srx_soft']) > 1 for r in rows)}")
    print(
        f"SOFT SRX set == elink SRX set: {sum(r['srx_soft'] == r['srx_elink'] for r in rows)}/{n}"
    )
    print(f"efetch packages silently dropped on first try: {dropped}")
    print(
        f"GSM whose SRX all resolve: {sum(r['srx_found'] == len(r['srx_soft']) for r in with_srx)}"
    )
    for key in ("alias_is_gsm", "center_geo", "biosample_agree"):
        print(f"{key}: {sum(bool(r[key]) for r in with_srx)}/{len(with_srx)}")
    print("runs per GSM:", dict(Counter(r["runs"] for r in with_srx)))
    print("layouts:", dict(Counter(tuple(r["layout"]) for r in with_srx)))
    for f in ("strategy", "source", "selection", "instrument", "taxid"):
        ok = sum(bool(r[f"{f}_agree"]) for r in with_srx)
        bad = Counter(
            p for r in with_srx if not r[f"{f}_agree"] for p in map(tuple, r[f"{f}_pairs"])
        )
        print(f"{f} agree: {ok}/{len(with_srx)}; disagreements: {dict(bad)}")
    for name in ("biosample", "sra_sample"):
        rs = [r for r in rows if r.get(f"char_in_{name}") is not None]
        pairs = sum(r["n_char"] for r in rs)
        exact = sum(r[f"char_in_{name}"] for r in rs)
        values = sum(r[f"char_values_in_{name}"] for r in rs)
        full = sum(r[f"char_in_{name}"] == r["n_char"] for r in rs)
        print(
            f"characteristics in {name}: {len(rs)} GSMs, {pairs} key:value pairs, "
            f"{exact} exact key+value, {values} value-only, {full} GSMs fully round-trip"
        )
        print(
            "  extra keys:",
            dict(Counter(k for r in rs for k in r[f"extra_in_{name}"]).most_common(8)),
        )


if __name__ == "__main__":
    main()
