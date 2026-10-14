"""PROTOTYPE. Fetch the slice records live from NCBI and write store/records.json.

Run from the repo root:  python prototypes/python_api/fetch_fixture.py

Sources, as decided on #26: family SOFT for GEO records, SRA's own XML per experiment for
SRA records, efetch XML for BioProject and BioSample, Entrez links for GSM -> SRX, and
PubMed's [si] field for paper links. At most 2 requests per second, backing off on HTTP 429.

Credentials come from NCBI_EMAIL / NCBI_API_KEY or the per-user ncbi.toml. Without them
the script runs anonymously, which NCBI allows at 3 requests per second.
"""

from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
import os
import re
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "store" / "records.json"
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"

# The slice. Each tuple: Series accession, GSMs to keep (None = first 2 in the file).
SERIES = [
    ("GSE246310", []),  # SuperSeries: keep its record, its samples come via SubSeries
    ("GSE246307", None),
    ("GSE246308", None),
    ("GSE246309", None),
    ("GSE19456", ["GSM484820", "GSM484821"]),
]
SRA_ONLY_PROJECT = "PRJNA1027859"

_last = 0.0


def _creds() -> dict[str, str]:
    email, key = os.environ.get("NCBI_EMAIL"), os.environ.get("NCBI_API_KEY")
    if not email:
        base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
        p = Path(base) / "liulab-data" / "ncbi.toml"
        if p.exists():
            d = tomllib.loads(p.read_text())
            email, key = d.get("email"), key or d.get("api_key")
    out = {"tool": "labdata-prototype"}
    if email:
        out["email"] = email
    if key:
        out["api_key"] = key
    return out


CREDS = _creds()


def get(url: str) -> bytes:
    global _last
    for attempt in range(5):
        wait = 0.5 - (time.monotonic() - _last)
        if wait > 0:
            time.sleep(wait)
        _last = time.monotonic()
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code != 429 or attempt == 4:
                raise
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def eutil(name: str, **params: str | list[str]) -> bytes:
    q = urllib.parse.urlencode({**params, **CREDS}, doseq=True)
    return get(f"{EUTILS}{name}.fcgi?{q}")


def sha(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


# ---------------------------------------------------------------- GEO (family SOFT)

SOFT_TYPES = {"SERIES": "GEO Series", "SAMPLE": "GEO Sample", "PLATFORM": "GEO Platform"}
_MON = {m: i for i, m in enumerate("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), 1)}


def _soft_date(s: str | None) -> str | None:
    if not s:
        return None
    m = re.search(r"([A-Z][a-z]{2}) (\d{1,2}) (\d{4})", s)
    return f"{m[3]}-{_MON[m[1]]:02d}-{int(m[2]):02d}" if m else None


def soft_blocks(text: str) -> list[tuple[str, str, str]]:
    """Split a family SOFT file into (entity, accession, raw block text)."""
    out, cur = [], None
    for line in text.splitlines(keepends=True):
        if line.startswith("^"):
            ent, _, acc = line[1:].partition(" = ")
            cur = [ent, acc.strip(), [line]]
            out.append(cur)
        elif cur:
            cur[2].append(line)
    return [(e, a, "".join(ls)) for e, a, ls in out]


def soft_record(ent: str, acc: str, raw: str) -> dict:
    fields = []
    for line in raw.splitlines():
        if line.startswith("!") and " = " in line:
            k, _, v = line[1:].partition(" = ")
            fields.append([k, v])
        elif line.startswith("!") and not line.startswith(("!sample_table", "!platform_table")):
            fields.append([line[1:].rstrip(" ="), ""])
    pre = {"SERIES": "Series", "SAMPLE": "Sample", "PLATFORM": "Platform"}[ent]
    first = {k: v for k, v in reversed(fields)}
    status = first.get(f"{pre}_status")
    return {
        "accession": acc,
        "type": SOFT_TYPES[ent],
        "source": "SOFT",
        "typed": {
            "status": status.split(" on ")[0] if status else None,
            "submitted": _soft_date(first.get(f"{pre}_submission_date")),
            "updated": _soft_date(first.get(f"{pre}_last_update_date")),
            "published": _soft_date(status),
        },
        "fields": fields,
        "raw_text": raw,
    }


_REL_URL = re.compile(r"(?:term=|/)((?:SRX|SRP|SRR|PRJNA|SAMN)\d+)\s*$")


def soft_links(rec: dict) -> list[dict]:
    acc, links = rec["accession"], []

    def add(frm: str, to: str, rel: str) -> None:
        links.append({"from": frm, "to": to, "relation": rel, "source": "SOFT", "stated_in": acc})

    for k, v in rec["fields"]:
        if k == "Series_relation":
            label, _, rest = v.partition(": ")
            if label == "SuperSeries of":
                add(acc, rest.strip(), "SuperSeries of")
            elif label == "SubSeries of":
                add(rest.strip(), acc, "SuperSeries of")
            elif label in ("BioProject", "SRA") and (m := _REL_URL.search(rest)):
                add(acc, m[1], label)
        elif k == "Series_sample_id":
            add(acc, v, "sample")
        elif k == "Sample_series_id":
            add(v, acc, "sample")
        elif k in ("Series_platform_id", "Sample_platform_id"):
            add(acc, v, "platform")
        elif k == "Sample_relation":
            label, _, rest = v.partition(": ")
            if label in ("BioSample", "SRA") and (m := _REL_URL.search(rest)):
                add(acc, m[1], label)
        elif k == "Series_pubmed_id":
            add(acc, f"PMID:{v}", "PubMed")
    return links


def fetch_soft(gse: str) -> str:
    url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse[:-3]}nnn/{gse}/soft/{gse}_family.soft.gz"
    return gzip.decompress(get(url)).decode()


# ---------------------------------------------------------------- SRA, BioProject, BioSample


def flatten(el: ET.Element, path: str = "") -> list[list[str]]:
    """Ordered (path, value) pairs: attributes as path@name, then text, then children."""
    p = f"{path}/{el.tag}" if path else el.tag
    out = [[f"{p}@{k}", v] for k, v in el.attrib.items()]
    if el.text and el.text.strip():
        out.append([p, el.text])
    for ch in el:
        out.extend(flatten(ch, p))
    return out


def slices(xml: str, tag: str) -> list[str]:
    return re.findall(rf"<{tag}[ >].*?</{tag}>", xml, flags=re.S)


def xml_record(acc: str, rtype: str, source: str, raw: str, typed: dict) -> dict:
    el = ET.fromstring(raw)
    return {
        "accession": acc,
        "type": rtype,
        "source": source,
        "typed": typed,
        "fields": flatten(el),
        "raw_text": raw,
    }


def _date(s: str | None) -> str | None:
    return s[:10] if s else None


def sra_records(xml: str) -> tuple[list[dict], list[dict]]:
    recs, links = [], []

    def link(frm: str, to: str, rel: str, stated_in: str) -> None:
        links.append(
            {"from": frm, "to": to, "relation": rel, "source": "SRA XML", "stated_in": stated_in}
        )

    none = {"status": None, "submitted": None, "updated": None, "published": None}
    for pkg in slices(xml, "EXPERIMENT_PACKAGE"):
        exp_raw = slices(pkg, "EXPERIMENT")[0]
        exp = ET.fromstring(exp_raw)
        srx = exp.get("accession")
        recs.append(xml_record(srx, "SRA experiment", "SRA XML", exp_raw, dict(none)))
        study_ref = exp.find("STUDY_REF")
        if study_ref is not None:
            link(srx, study_ref.get("accession"), "study", srx)
        samp_ref = exp.find("DESIGN/SAMPLE_DESCRIPTOR")
        if samp_ref is not None:
            link(srx, samp_ref.get("accession"), "SRA sample", srx)
        for raw in slices(pkg, "STUDY"):
            st = ET.fromstring(raw)
            srp = st.get("accession")
            recs.append(xml_record(srp, "SRA study", "SRA XML", raw, dict(none)))
            for x in st.iter("EXTERNAL_ID"):
                if x.get("namespace") == "BioProject":
                    link(srp, x.text, "BioProject", srp)
        for raw in slices(pkg, "SAMPLE"):
            sm = ET.fromstring(raw)
            srs = sm.get("accession")
            recs.append(xml_record(srs, "SRA sample", "SRA XML", raw, dict(none)))
            for x in sm.iter("EXTERNAL_ID"):
                if x.get("namespace") == "BioSample":
                    link(srs, x.text, "BioSample", srs)
        for raw in slices(pkg, "RUN"):
            run = ET.fromstring(raw)
            srr = run.get("accession")
            typed = dict(
                none,
                published=_date(run.get("published")),
                status="public" if run.get("is_public") == "true" else None,
            )
            recs.append(xml_record(srr, "SRA run", "SRA XML", raw, typed))
            link(srx, srr, "run", srr)
    return recs, links


def bioproject_records(xml: str) -> tuple[list[dict], list[dict]]:
    recs, links = [], []
    for raw in slices(xml, "DocumentSummary"):
        el = ET.fromstring(raw)
        acc = el.find(".//ArchiveID").get("accession")
        sub = el.find("Submission")
        typed = {
            "status": None,
            "submitted": _date(sub.get("submitted") if sub is not None else None),
            "updated": _date(sub.get("last_update") if sub is not None else None),
            "published": _date(el.findtext(".//ProjectReleaseDate")),
        }
        recs.append(xml_record(acc, "BioProject", "BioProject XML", raw, typed))
        for ln in el.iter("Link"):
            child, top = ln.find("ProjectIDRef"), ln.find("Hierarchical/MemberID")
            if (
                child is not None
                and top is not None
                and ln.find("Hierarchical").get("type") == "TopAdmin"
            ):
                links.append(
                    {
                        "from": top.get("accession"),
                        "to": child.get("accession"),
                        "relation": "umbrella of",
                        "source": "BioProject XML",
                        "stated_in": acc,
                    }
                )
        for pub in el.iter("Publication"):
            if pub.get("id") and pub.findtext("DbType") == "ePubmed":
                links.append(
                    {
                        "from": acc,
                        "to": f"PMID:{pub.get('id')}",
                        "relation": "PubMed",
                        "source": "BioProject XML",
                        "stated_in": acc,
                    }
                )
    return recs, links


def biosample_records(xml: str) -> list[dict]:
    recs = []
    for raw in slices(xml, "BioSample"):
        el = ET.fromstring(raw)
        st = el.find("Status")
        typed = {
            "status": st.get("status") if st is not None else None,
            "submitted": _date(el.get("submission_date")),
            "updated": _date(el.get("last_update")),
            "published": _date(el.get("publication_date")),
        }
        recs.append(xml_record(el.get("accession"), "BioSample", "BioSample XML", raw, typed))
    return recs


def esearch(db: str, term: str) -> list[str]:
    js = json.loads(eutil("esearch", db=db, term=term, retmax="500", retmode="json"))
    return js["esearchresult"]["idlist"]


# ---------------------------------------------------------------- main


def main() -> None:
    today = dt.datetime.now(dt.UTC).date().isoformat()
    records: dict[str, dict] = {}
    links: list[dict] = []
    copies: dict[str, set[str]] = {}  # accession -> hashes of every copy seen

    def put(rec: dict) -> None:
        rec["content_hash"] = sha(rec["raw_text"])  # over the block exactly as served
        if "!sample_table_begin" in rec["raw_text"]:
            # Keep the fixture small: drop the data table rows, keep the begin/end lines.
            head, _, rest = rec["raw_text"].partition("!sample_table_begin\n")
            table, _, tail = rest.partition("!sample_table_end")
            rec["raw_text"] = head + "!sample_table_begin\n" + "!sample_table_end" + tail
            rec["table_lines_omitted"] = table.count("\n")
        rec["snapshot_date"] = today
        copies.setdefault(rec["accession"], set()).add(rec["content_hash"])
        records.setdefault(rec["accession"], rec)

    keep_gsm: list[str] = []
    for gse, keep in SERIES:
        blocks = soft_blocks(fetch_soft(gse))
        gsms = [a for e, a, _ in blocks if e == "SAMPLE"]
        want = set(gsms[:2] if keep is None else keep)
        keep_gsm += [g for g in gsms if g in want]
        for ent, acc, raw in blocks:
            if ent not in SOFT_TYPES or (ent == "SAMPLE" and acc not in want):
                continue
            rec = soft_record(ent, acc, raw)
            put(rec)
            links += soft_links(rec)

    # GSM -> SRX through Entrez links (GSM uid in gds is 300000000 + its number).
    ids = [str(300000000 + int(g[3:])) for g in keep_gsm]
    js = json.loads(eutil("elink", dbfrom="gds", db="sra", id=ids, retmode="json"))
    sra_uid_of: dict[str, list[str]] = {}
    for ls in js["linksets"]:
        gsm = f"GSM{int(ls['ids'][0]) - 300000000}"
        for db in ls.get("linksetdbs", []):
            if db["linkname"] == "gds_sra":
                sra_uid_of[gsm] = db["links"]
    all_uids = sorted({u for us in sra_uid_of.values() for u in us})
    summ = json.loads(eutil("esummary", db="sra", id=",".join(all_uids), retmode="json"))
    uid_acc = {
        u: re.search(r'<Experiment acc="([^"]+)"', summ["result"][u]["expxml"])[1] for u in all_uids
    }
    for gsm, us in sra_uid_of.items():
        for u in us:
            links.append(
                {
                    "from": gsm,
                    "to": uid_acc[u],
                    "relation": "SRA",
                    "source": "Entrez link",
                    "stated_in": gsm,
                }
            )

    # SRA XML: the union of SOFT- and Entrez-stated SRX, plus the SRA-only project.
    srx = sorted({x["to"] for x in links if x["relation"] == "SRA" and x["to"].startswith("SRX")})
    uids = set(all_uids)
    uids |= set(esearch("sra", " OR ".join(f"{a}[accn]" for a in srx)))
    uids |= set(esearch("sra", SRA_ONLY_PROJECT))
    recs, ls = sra_records(eutil("efetch", db="sra", id=",".join(sorted(uids))).decode())
    for r in recs:
        put(r)
    links += ls

    # BioProject for every project a held record names; BioSample for the SRA-only slice.
    prj = sorted({x["to"] for x in links if x["relation"] == "BioProject"} | {SRA_ONLY_PROJECT})
    bp_uids = esearch("bioproject", " OR ".join(f"{p}[PRJA]" for p in prj))
    recs, ls = bioproject_records(eutil("efetch", db="bioproject", id=",".join(bp_uids)).decode())
    for r in recs:
        put(r)
    links += ls
    srs_of_prj = {
        x["from"] for x in links if x["relation"] == "BioProject" and x["to"] == SRA_ONLY_PROJECT
    }
    srx_of_prj = {x["from"] for x in links if x["relation"] == "study" and x["to"] in srs_of_prj}
    srs = {x["to"] for x in links if x["relation"] == "SRA sample" and x["from"] in srx_of_prj}
    samn = sorted({x["to"] for x in links if x["relation"] == "BioSample" and x["from"] in srs})
    bs_uids = esearch("biosample", " OR ".join(samn))
    for r in biosample_records(eutil("efetch", db="biosample", id=",".join(bs_uids)).decode()):
        put(r)

    # PubMed's own accession field, one query per Series and per project.
    for acc in [g for g, _ in SERIES] + [SRA_ONLY_PROJECT]:
        for pmid in esearch("pubmed", f"{acc}[si]"):
            links.append(
                {
                    "from": acc,
                    "to": f"PMID:{pmid}",
                    "relation": "PubMed",
                    "source": "PubMed",
                    "stated_in": f"PMID:{pmid}",
                }
            )

    uniq = {json.dumps(x, sort_keys=True): x for x in links}
    links = sorted(uniq.values(), key=lambda x: (x["from"], x["relation"], x["to"], x["source"]))
    multi = {a: sorted(h) for a, h in copies.items() if len(h) > 1}
    out = {
        "snapshot_date": today,
        "records": sorted(records.values(), key=lambda r: r["accession"]),
        "links": links,
        "copies_that_differ": multi,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
    print(
        f"{len(records)} records, {len(links)} links, {len(multi)} accessions with differing "
        f"copies -> {OUT.relative_to(HERE.parent.parent)}"
    )


if __name__ == "__main__":
    main()
