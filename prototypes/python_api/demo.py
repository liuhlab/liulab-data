"""PROTOTYPE demo of the offline store API (#28).

Run from the repo root: pixi run python prototypes/python_api/demo.py
"""

from __future__ import annotations

import os
import tempfile
from collections import Counter
from pathlib import Path

from labstore import NotAStore, NotMirrored, StoreNotConfigured, open_store

HERE = Path(__file__).resolve().parent


def section(title: str) -> None:
    print(f"\n=== {title}")


def calls(store) -> str:
    """Backend calls since the last reset, then reset."""
    c = dict(store.backend.calls)
    store.backend.calls.clear()
    return f"[backend calls: {c}]"


def show(rec, n_fields: int = 6) -> None:
    print(f"{rec.accession}  type={rec.type!r}  source={rec.source!r}")
    print(
        f"  status={rec.status!r} submitted={rec.submitted} updated={rec.updated} "
        f"published={rec.published}"
    )
    print(f"  content_hash={rec.content_hash[:23]}...  snapshot_date={rec.snapshot_date}")
    print(f"  {len(rec.fields)} raw fields, first {n_fields}:")
    for k, v in rec.fields[:n_fields]:
        print(f"    {k} = {v[:70]}")


def show_links(rec, limit: int = 12) -> None:
    print(f"  {len(rec.links)} links (-> forward, <- read from the far end):")
    for lk in rec.links[:limit]:
        arrow = "->" if lk.forward else "<-"
        print(f"    {arrow} {lk.relation:<15} {lk.other:<14} [{lk.source}, in {lk.stated_in}]")
    if len(rec.links) > limit:
        print(f"    ... {len(rec.links) - limit} more")


section("1. open_store: root from argument, then $LABDATA_STORE, then the per-user file")
os.environ.pop("LABDATA_STORE", None)
os.environ["XDG_CACHE_HOME"] = tempfile.mkdtemp()  # hide any real per-user pointer
try:
    open_store()
except StoreNotConfigured as e:
    print(f"open_store()                 -> StoreNotConfigured: {e}")
try:
    open_store(root=HERE)
except NotAStore as e:
    print(f"open_store(root=<no marker>) -> NotAStore: {e}")
os.environ["LABDATA_STORE"] = str(HERE / "store")
store = open_store()
print(f"LABDATA_STORE=.../store; open_store() -> {store!r}")
calls(store)

section("2. store.experiments_for(acc), the offline twin of labdata.experiments_for")
for acc in ["PRJNA1027859", "GSM484820", "SRR28716558"]:
    exps = store.experiments_for(acc)
    print(f"{acc:<13} -> {[e.accession for e in exps]}")
try:
    store.experiments_for("GSE246310")
except NotMirrored as e:
    print(f"GSE246310     -> NotMirrored: {e}")
exps = store.experiments_for("GSE246310", partial=True)
print(f"GSE246310, partial=True -> {[e.accession for e in exps]}")
calls(store)

section("3. store.open(acc): a GEO Sample whose one GSM has two SRA experiments")
gsm = store.open("GSM484820")
show(gsm)
show_links(gsm)
print(f"  gsm.field('Sample_characteristics_ch1') = {gsm.field('Sample_characteristics_ch1')}")
print(f"  gsm.follow('SRA') = {gsm.follow('SRA')}")
try:
    gsm.fields[0] = ("x", "y")  # type: ignore[index]
except TypeError as e:
    print(f"  records are frozen: {e}")
print(f"  {calls(store)}")

section("4. Following links both ways (SRA-only study PRJNA1027859)")
run = store.open(store.experiments_for("PRJNA1027859")[0].follow("run")[0])
show(run, n_fields=4)
srx = store.open(run.follow("run of")[0])
srp = store.open(srx.follow("study")[0])
prj = store.open(srp.follow("BioProject")[0])
print(
    f"{run.accession} -run of-> {srx.accession} -study-> {srp.accession} "
    f"-BioProject-> {prj.accession}"
)
print(f"{prj.accession} -BioProject of-> {prj.follow('BioProject of')}")
print(
    f"{prj.accession} -PubMed-> "
    f"{[(lk.other, lk.source) for lk in prj.links if lk.relation == 'PubMed']}"
)
srs = store.open(srx.follow("SRA sample")[0])
bs = store.open(srs.follow("BioSample")[0])
print(
    f"{srx.accession} -SRA sample-> {srs.accession} -BioSample-> {bs.accession} "
    f"(status={bs.status!r}, published={bs.published})"
)
calls(store)

section("5. SuperSeries <-> SubSeries, BioProject umbrella, one GSM in two Series")
sub = store.open("GSE246307")
print(f"GSE246307 -SubSeries of-> {sub.follow('SubSeries of')}")
for lk in sub.links:
    if lk.relation == "SubSeries of":
        print(f"    stated by {lk.source} in {lk.stated_in}")
sup = store.open("GSE246310")
umb = store.open(sup.follow("BioProject")[0])
print(f"GSE246310 -SuperSeries of-> {sup.follow('SuperSeries of')}")
print(f"GSE246310 -BioProject-> {umb.accession} -umbrella of-> {umb.follow('umbrella of')}")
print("GSM484820's two experiments lead to two SRA studies and two BioProjects:")
for x in gsm.follow("SRA"):
    e = store.open(x)
    st = store.open(e.follow("study")[0])
    print(
        f"  {x} -study-> {st.accession} -BioProject-> {st.follow('BioProject')} "
        f"-GEO-> {st.follow('GEO')}"
    )
print(f"  GSM484820 -series-> {gsm.follow('series')}")
calls(store)

section("6. NotMirrored vs NotFound")
res = store.open_many(["GSE246309", "SAMN00010819", "GSM9999999999", "GSE1x"])
for acc, r in res.items():
    print(f"{acc:<14} -> {type(r).__name__}: {r.type if hasattr(r, 'type') else r}")
print(calls(store))

section("7. Bulk paths over the whole fixture: one backend call per step")
every = store.backend.all_accessions()
calls(store)
recs = store.open_many(every)
print(
    f"open_many({len(every)} accessions) -> {len(recs)} records "
    f"{Counter(type(r).__name__ for r in recs.values())}  {calls(store)}"
)

edges = store.links(every)
print(f"links({len(every)} accessions) -> Frame of {len(edges)} link rows  {calls(store)}")
print(edges.head(4))

srx_all = [a for a in every if a.startswith("SRX")]
runs = store.follow(srx_all, "run")
print(
    f"follow({len(srx_all)} SRX, 'run') -> "
    f"{sum(len(v) for v in runs.values())} runs  {calls(store)}"
)
gsm_all = [a for a in every if a.startswith("GSM")]
geo = store.follow(gsm_all, "SRA")
print(f"follow({len(gsm_all)} GSM, 'SRA') -> {geo}  {calls(store)}")

roots = ["PRJNA1027859", "GSE246307", "GSE246308", "GSE246309", "GSM484820", "GSM484821"]
many = store.experiments_for_many(roots, partial=True)
print(
    f"experiments_for_many({len(roots)} roots, partial=True) -> "
    f"{ {k: len(v) for k, v in many.items()} }  {calls(store)}"
)
print("  (one links_many + one get_many per level of the walk, for all roots together)")

section("8. One query across the store, columnar")
LS = "EXPERIMENT/DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_STRATEGY"
INST = "EXPERIMENT/PLATFORM/ILLUMINA/INSTRUMENT_MODEL"
t = store.table("SRA experiment", [LS, INST], typed=())
print(
    f"table('SRA experiment', [LIBRARY_STRATEGY, INSTRUMENT_MODEL]) -> "
    f"{len(t)} rows  {calls(store)}"
)
for (strategy, inst), n in sorted(
    Counter(zip(t.columns[LS], t.columns[INST], strict=True)).items()
):
    print(f"  {n:>2}  {strategy:<10} {inst}")
g = store.table(
    "GEO Sample",
    ["Sample_library_strategy", "Sample_organism_ch1"],
    where=lambda r: r["Sample_library_strategy"] == "RNA-Seq",
)
print(f"table('GEO Sample', ..., where=RNA-Seq) -> {len(g)} rows  {calls(store)}")
print(g.head())
