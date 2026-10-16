"""Run the stage 1 acceptance checks against a real store with the network blocked.

Usage: ``pixi run python scripts/store/check_stage1.py [ROOT]``. ROOT defaults to the configured
store (``LABDATA_STORE``, then the pointer file).
"""

import socket
from collections.abc import Callable, Iterator
from typing import Any

ATTEMPTS: list[str] = []


def _refuse(*args: object, **kwargs: object) -> Any:
    ATTEMPTS.append(repr(args[:2]))
    raise OSError("network blocked by acceptance check")


socket.socket.connect = _refuse  # type: ignore[method-assign]
socket.socket.connect_ex = _refuse  # type: ignore[method-assign]
socket.create_connection = _refuse  # type: ignore[assignment]
socket.getaddrinfo = _refuse  # type: ignore[assignment]

import hashlib  # noqa: E402
import re  # noqa: E402
import sys  # noqa: E402
import traceback  # noqa: E402
from functools import cached_property  # noqa: E402
from pathlib import Path  # noqa: E402

import pyarrow.parquet as pq  # noqa: E402

from labdata.exceptions import AccessionError, NotFound, NotMirrored  # noqa: E402
from labdata.geo import Experiment  # noqa: E402
from labdata.store import open_store, raw  # noqa: E402
from labdata.store.model import Record  # noqa: E402

store = open_store(Path(sys.argv[1]) if len(sys.argv) > 1 else None)
ROOT = store.root
BUILD = ROOT / "builds" / str(store.settings["build"])
RESULTS: list[tuple[str, bool, str]] = []


def check(name: str) -> Callable[[Callable[[], object]], Callable[[], object]]:
    """Run the decorated check now and record its result under ``name``."""

    def wrap(fn: Callable[[], object]) -> Callable[[], object]:
        try:
            detail = fn()
            RESULTS.append((name, True, str(detail or "")))
        except Exception as exc:
            RESULTS.append((name, False, f"{type(exc).__name__}: {exc}"))
            traceback.print_exc()
        return fn

    return wrap


def eq(observed: object, expected: object, what: str = "") -> None:
    """Raise ``AssertionError`` unless ``observed == expected``."""
    if observed != expected:
        raise AssertionError(f"{what} observed {observed!r} expected {expected!r}")


def sources(acc: str, relation: str) -> dict[str, list[str]]:
    """Map each far end of ``relation`` from ``acc`` to its sorted link sources."""
    out: dict[str, list[str]] = {}
    for link in store.open(acc).links:
        if link.relation == relation:
            out.setdefault(link.other, []).append(link.source)
    return {k: sorted(v) for k, v in sorted(out.items())}


SKIP = {"files", "original_files"}


def read_all(obj: object) -> dict[str, object]:
    """Read every property and cached_property of obj, except files/original_files."""
    seen: dict[str, object] = {}
    for cls in type(obj).__mro__:
        for name, attr in vars(cls).items():
            if name in SKIP or name in seen:
                continue
            if isinstance(attr, (property, cached_property)):
                seen[name] = getattr(obj, name)
    return seen


# ------------------------------------------------------------ section 1: done check
@check("done: PRJNA1027859 -> 6 Experiments, every property, runs, SRS, SAMN")
def _done() -> str:
    exps = store.experiments_for("PRJNA1027859")
    eq(len(exps), 6, "experiments")
    assert all(type(e) is Experiment for e in exps)
    nprops = 0
    runs = []
    for e in exps:
        nprops += len(read_all(e))
        for r in e.runs:
            nprops += len(read_all(r))
            runs.append(r.accession)
    srx = [e.accession for e in exps]
    for acc in srx + runs:
        store.open(acc)
    srs = store.follow(srx, "SRA sample")
    assert all(len(v) == 1 for v in srs.values()), srs
    samn = store.follow([v[0] for v in srs.values()], "BioSample")
    assert all(len(v) == 1 for v in samn.values()), samn
    recs = store.open_many([v[0] for v in srs.values()] + [v[0] for v in samn.values()])
    assert all(isinstance(r, Record) for r in recs.values()), recs
    return (
        f"SRX={srx}; runs={len(runs)}; props read={nprops}; "
        f"SRS={len(srs)}; SAMN={sorted(v[0] for v in samn.values())}; "
        f"studies={sorted({e.study for e in exps})}"
    )


# ------------------------------------------------------------ section 7
@check("1 PRJNA1027859")
def _r1() -> str:
    exps = store.experiments_for("PRJNA1027859")
    eq(len(exps), 6)
    pm = sources("PRJNA1027859", "PubMed")
    eq(pm, {"PMID:39637862": ["PubMed"]}, "PubMed")
    srx = [e.accession for e in exps]
    srs = store.follow(srx, "SRA sample")
    samn = store.follow([s for v in srs.values() for s in v], "BioSample")
    assert all(len(v) == 1 for v in srs.values())
    assert all(len(v) == 1 for v in samn.values())
    return f"6 exps; PubMed={pm}; each SRX->1 SRS->1 SAMN"


@check("2 GSE246310 + 3 SubSeries")
def _r2() -> str:
    subs = ["GSE246307", "GSE246308", "GSE246309"]
    eq(sources("GSE246310", "SuperSeries of"), {s: ["SOFT"] for s in subs}, "SuperSeries of")
    for s in subs:
        eq(sources(s, "SubSeries of"), {"GSE246310": ["SOFT"]}, f"{s} SubSeries of")
    bp = store.follow("GSE246310", "BioProject")["GSE246310"]
    eq(len(bp), 1, "Series BioProject")
    kids = store.follow(bp[0], "umbrella of")[bp[0]]
    eq(len(kids), 3, "umbrella children")
    sub_bps = {s: store.follow(s, "BioProject")[s] for s in subs}
    # GSMs in both a SubSeries and the SuperSeries
    soft = {
        r.accession: r.raw_text
        for r in raw.read_rows(ROOT, raw.GEO_SOFT)
        if r.accession in [*subs, "GSE246310"]
    }
    super_gsm = set(store.follow("GSE246310", "sample")["GSE246310"])
    shared = []
    for s in subs:
        for gsm in store.follow(s, "sample")[s]:
            if gsm in super_gsm:
                shared.append(gsm)
    texts = store.raw(shared)
    for gsm in shared:
        sub = [s for s in subs if gsm in store.follow(s, "sample")[s]]
        t = texts[gsm].text
        assert all(t in soft[s] for s in sub), f"{gsm} not in its SubSeries text"
        assert t in soft["GSE246310"], f"{gsm} copy differs in SuperSeries"
    assert shared, "no GSM shared"
    return f"umbrella {bp[0]} of {kids}; SubSeries BPs={sub_bps}; {len(shared)} shared GSMs agree"


@check("3 GSE153830")
def _r3() -> str:
    bp = sources("GSE153830", "BioProject")
    eq(list(bp), ["PRJNA644323"], "Series BioProject")
    exps = store.experiments_for("GSE153830")
    studies = sorted({e.study for e in exps})
    sbp = store.follow(studies, "BioProject")
    far = {b for v in sbp.values() for b in v}
    assert far, far
    assert "PRJNA644323" not in far, far
    return f"Series BP={bp}; {len(exps)} exps; studies {studies} -> {sorted(far)}"


@check("4 GSE19456")
def _r4() -> str:
    s = sources("GSM484820", "SRA")
    eq(
        s,
        {"SRX018673": ["Entrez link", "SOFT"], "SRX018710": ["Entrez link", "SOFT"]},
        "GSM484820 SRA",
    )
    st = store.follow(["SRX018673", "SRX018710"], "study")
    studies = sorted({x for v in st.values() for x in v})
    eq(len(studies), 2, "studies")
    ser = store.follow("GSM484820", "series")["GSM484820"]
    assert "GSE19970" in ser, ser
    try:
        store.open("GSE19970")
        raise AssertionError("GSE19970 opened")
    except NotMirrored as exc:
        msg = str(exc)
    return f"SRA={s}; studies={studies}; series={ser}; NotMirrored: {msg[:80]}"


@check("5 PRJNA436361")
def _r5() -> str:
    pm = sources("PRJNA436361", "PubMed")
    assert any("BioProject XML" in v for v in pm.values()), pm
    return f"PubMed={pm}"


@check("6 PRJNA530347")
def _r6() -> str:
    eq(store.follow("PRJNA530347", "PubMed"), {"PRJNA530347": []}, "PubMed")
    saved = raw.saved(ROOT, raw.PUBMED_SI, ["PRJNA530347"])
    eq(saved, {"PRJNA530347": ""}, "raw [si]")
    return f"no PubMed; raw [si]={saved}"


@check("7,8 GSE233931 + GSE233685")
def _r78() -> str:
    out = {}
    for g in ("GSE233931", "GSE233685"):
        out[g] = sources(g, "PubMed")
        eq(store.follow(g, "SuperSeries of"), {g: []})
        eq(store.follow(g, "SubSeries of"), {g: []})
    eq(out["GSE233931"], out["GSE233685"], "same PMID")
    assert all(v == ["SOFT"] for d in out.values() for v in d.values()), out
    assert out["GSE233931"], out
    return f"{out}; no SuperSeries link"


@check("9 GSE20301")
def _r9() -> str:
    eq(store.follow(["GSM517436", "GSM517440"], "SRA"), {"GSM517436": [], "GSM517440": []})
    gsms = store.follow("GSE20301", "sample")["GSE20301"]
    sra = store.follow(gsms, "SRA")
    four = {g: v for g, v in sra.items() if len(v) >= 4}
    assert four, sra
    exps = store.experiments_for("GSE20301")
    return f"{len(gsms)} GSMs; >=4 SRX: {four}; experiments_for -> {len(exps)}"


@check("10 GSE15556")
def _r10() -> str:
    eq(store.experiments_for("GSE15556"), [], "experiments")
    gsms = store.follow("GSE15556", "sample")["GSE15556"]
    files = store.supplementary_files(["GSE15556", *gsms])
    flat = [f for v in files.values() for f in v]
    reads = [
        f
        for f in flat
        if f.archive or re.search(r"\.(fa|fasta|fq|fastq|sff)(\.gz)?$", f.name, re.I)
    ]
    assert reads, reads
    assert all(f.size and f.date for f in reads), reads
    s = store.open("GSE15556")
    tax = s.field("Series_sample_taxid")
    eq(tax, ["8355", "8364"], "Series_sample_taxid")
    first = reads[0]
    return (
        f"[]; {len(reads)} read files w/ size+date e.g. {first.name} {first.size} {first.date}; "
        f"taxid={tax}"
    )


@check("11 GSE28770")
def _r11() -> str:
    exps = store.experiments_for("GSE28770")
    studies = sorted({e.study for e in exps})
    eq(len(studies), 3, f"studies {studies}")
    eq(store.follow(studies, "BioProject"), {s: [] for s in studies}, "study BioProject")
    bp = sources("GSE28770", "BioProject")
    eq(len(bp), 1)
    eq(list(bp.values()), [["Entrez link"]], "BP source")
    [prj] = bp
    kids = store.follow(prj, "umbrella of")[prj]
    return f"studies={studies}; BP={bp}; umbrella of {len(kids)} children"


# ------------------------------------------------------------ every record
held = pq.read_table(BUILD / "holdings.parquet").column("accession").to_pylist()
# Records and raw texts are checked a chunk at a time, so memory stays flat as the store grows.
CHUNK = 20_000


def chunks() -> Iterator[list[str]]:
    """Yield the held accessions in lists of at most ``CHUNK``."""
    for start in range(0, len(held), CHUNK):
        yield held[start : start + CHUNK]


@check("every: open all held")
def _open() -> str:
    bad = []
    for part in chunks():
        opened = store.open_many(part)
        bad += [a for a, r in opened.items() if not isinstance(r, Record)]
    eq(bad, [], "failed opens")
    return f"{len(held)} held, all open"


@check("every: content_hash recomputes from store.raw")
def _hash() -> str:
    bad = []
    for part in chunks():
        raws = store.raw(part)
        opened = store.open_many(part)
        for a in part:
            rec = opened[a]
            if not (
                isinstance(rec, Record)
                and hashlib.sha256(raws[a].text.encode()).hexdigest()
                == raws[a].content_hash
                == rec.content_hash
            ):
                bad.append(a)
    eq(bad, [], "hash mismatch")
    return f"{len(held)} hashes match"


@check("every: malformed -> AccessionError")
def _malformed() -> str:
    for acc in ["GSE", "SRX12a", "PRJ1", "FOO1", "", "SAMN", "GSMx1"]:
        try:
            store.open(acc)
            raise AssertionError(f"{acc!r} opened")
        except AccessionError:
            pass
    return "7 malformed raise AccessionError"


@check("every: absent -> NotFound with date")
def _absent() -> str:
    msgs = []
    for acc in [
        "GSE999999999",
        "GSM999999999",
        "GPL999999999",
        "SRP999999999",
        "SRX999999999",
        "SRR999999999",
        "SRS999999999",
        "PRJNA999999999",
        "SAMN999999999",
    ]:
        try:
            store.open(acc)
            raise AssertionError(f"{acc} opened")
        except NotFound as exc:
            m = str(exc)
            assert acc in m, m
            assert re.search(r"\d{4}-\d{2}-\d{2}", m), m
            msgs.append(m)
    return msgs[0]


for name, ok, detail in RESULTS:
    print(f"{'PASS' if ok else 'FAIL'} | {name} | {detail}")
print(f"network attempts: {len(ATTEMPTS)} {ATTEMPTS[:5]}")
sys.exit(0 if all(ok for _, ok, _ in RESULTS) and not ATTEMPTS else 1)
