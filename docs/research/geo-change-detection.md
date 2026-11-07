---
search:
  exclude: true
---

# How a refresh finds changed GEO Series

Measured **2026-11-06** with small, gentle queries (no API key, under 3 requests per second)
against E-utilities and GEO's own record pages. The question: does GEO's last-update date, as
Entrez searches it, move for every change the store cares about? What does it miss, and what
cheap second signal catches the miss?

Evidence classes, as in the earlier notes. **Measured** means a query or script ran on
2026-11-06 and the number is its output. **Read** means an official page or API says it.
**Inferred** means reasoned from measured or read facts and not checked directly.

The pilots are in `scripts/research/geo_change/` (`es.py` is the shared helper; `exp.py` to
`exp4.py` are the four pilots below). They read the brief SOFT text of one record from
`https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=<ACC>&targ=self&form=text&view=brief`.

## Verdict

**Use the Entrez Update Date field, open-ended from the last refresh, with a few days of
overlap:**

```text
esearch db=gds term: gse[ETYP] AND <last refresh - 3 days>:3000[UDAT]
```

Intersect the result with the target list (the sequencing Series). Then re-fetch two more sets
the date does not cover:

1. **Every record named by a "Reanalysis of", "SuperSeries of" or "SubSeries of" line in a
   Series fetched this refresh.** A reanalysis released after its link was made adds a
   "Reanalyzed by" line to the original sample, and the original's date does not move at
   release (measured, one case, below). The refresh fetches these new Series anyway, so the
   targets cost nothing to find.
2. **The Series missing from the renewed `geo-series` snapshot** are withdrawn. A date
   search can't see a deletion (inferred: a deleted record has no date to search).

Diffing the old and new `geo-series` snapshot rows (sample count, sample list, relations,
supplementary file types, PubMed IDs) is a free backup check for the same Series, because a
refresh renews that snapshot anyway (inferred; not tested against a real miss).

**Cons:** the sample is small (about 300 records read). The relation miss rests on one
reanalysis Series. GEO repacked some `_RAW.tar` archives in place without moving the date
(3 of 120 Series, measured); the store's `filelist.txt` row for the archive then goes stale
in time and size, though no member file changed. Catching that would need a slow rolling
re-fetch of file lists, and it is probably not worth it.

## 1. What Entrez offers

- **The `gds` database has two searchable date fields: `PDAT` "Publication Date" and `UDAT`
  "Update Date"** (read: `einfo db=gds`, version 2.0, field list). There is no modification
  date in the `datetype=mdat` sense.
- **The `esummary` record of a Series has no update date** (measured, `esummary db=gds` for
  GSE135893). Its fields are `pdat`, `n_samples`, `samples`, `relations`, `extrelations`
  (the SRA link), `suppfile` (file types only), `pubmedids`, `bioproject` and titles. So the
  `geo-series` snapshot the store already renews can't give the date; only a search can.
- **The index is current to the day.** `einfo` gave a last-update stamp of 2026/11/06 09:47
  on the day of the queries (measured).
- **UDAT holds only the newest update.** In October 2026, 2,378 Series were released
  (`PDAT`), but only 2,369 of those also have an October `UDAT` (measured): the other 9 were
  updated again in November. So a fixed calendar-month window misses re-updated Series; the
  window must run from the last refresh to now (inferred from the counts).
- NCBI's E-utilities and GEO help pages answered with a robot check page when fetched from
  this machine, so the `datetype`, `mindate` and `maxdate` docs were not re-read for this
  note. The range syntax `<from>:<to>[UDAT]` is the documented Entrez search form and it
  worked (measured: Entrez echoed it as `2026/10/01[UDAT] : 2026/10/31[UDAT]`).

### Counts

| Query (all `gse[ETYP]`) | Count |
| --- | --- |
| All Series | 300,226 |
| `UDAT` in October 2026 | 4,226 |
| `PDAT` in October 2026 | 2,378 |
| `UDAT` in October, `PDAT` not in October | 1,857 |
| `UDAT` in October, `PDAT` not in 2026 | 599 |
| `UDAT` in September 2026 | 5,261 |
| `UDAT` in September, `PDAT` not in September | 2,740 |
| `UDAT` 2026/10/15 | 417 |
| `PDAT` 2026/10/15 | 124 |

All measured on 2026-11-06. A monthly refresh re-fetches roughly 2,000 to 3,000 old Series
across all of GEO, before cutting to sequencing Series (inferred from the table). The
`gdstype` phrase "high throughput sequencing" matches nothing as a `GTYP` phrase search (0
hits, measured), so filter on the target list, not in the query.

## 2. Does the search date match the record's own date?

`exp.py`, part A: of the 417 Series with `UDAT` 2026/10/15, a random 40 were read. **All 40
have `!Series_last_update_date = Oct 15 2026`** (measured).

`exp4.py`: 37 Series released on 2026/10/15 were absent from that day's `UDAT` list. Their
SOFT dates fell on five later days (Oct 16, 19, 26, 30 and Nov 4). **All 37 are in the `UDAT`
list for their own SOFT date** (measured). So the search date and the SOFT date agree on 77 of
77 Series across six days.

## 3. Which changes move the Series date

| Change | Moves the Series date? | Evidence |
| --- | --- | --- |
| Sample edited | Yes. The Series date is the same day or later | measured |
| Release from private | Yes. The date is on or after the release day | measured |
| New sample or other edit by the submitter | Yes, as above | inferred |
| Related record made (reanalysis, SuperSeries) | Yes, on the day the link is made | measured, 1 case |
| Linked reanalysis released later | **No** | measured, 1 case |
| GEO repacks `_RAW.tar` | **No** | measured, 3 of 120 |
| Contact details edited | Unknown; not biological metadata | read |

**Edited sample** (`exp.py`, part B). 40 random samples with `UDAT` 2026/10/15 link to 50
Series. 44 of those Series also carry Oct 15; the other 6 carry Oct 16 (measured). None
carries an earlier date. A sample edit therefore moves its Series, sometimes a day later. The
open-ended window covers that.

**Release from private** (`exp2.py`, part R). All 124 Series released on 2026/10/15 have a
last-update date on or after that day: 87 the same day, 37 later (measured). GEO's update page
says a Series' release date change propagates to its private samples (read: "Updating your
GEO records", page dated July 16, 2024, via a public mirror).

**Supplementary files** (`exp2.py`, part S). 120 random Series were read (drawn from three
random pages of the Series list, so most were last updated in 2019 to 2022). For each, the
dates in the `suppl/` folder listing were compared with the Series date. 115 had no file newer
than the Series date. **3 had `GSExxx_RAW.tar` and `filelist.txt` dated 2024-02-01, after
the Series date** (GSE159955, GSE159540, GSE157241; measured). In GSE159955's
`filelist.txt` the member files keep their 2020 dates; only the archive row is new. This looks
like a GEO-side repack on one day, not a submitter change (inferred). The `soft/` folder date is
also regenerated by GEO (GSE135893: folder 2026-08-21, Series date 2025-02-21; measured), so
download-site folder dates are no change signal.

**Relation lines.** In part S, 23 "SubSeries of" and 11 "SuperSeries of" partners were all
released on or before the Series' own date: no miss (measured). `exp3.py` read every Series
released on 2026/10/13 to 2026/10/15 for "Reanalysis of" lines and found one: GSE343291,
submitted Aug 11, released Oct 14, reanalysing 14 samples from GSE138001 and GSE79238. Each of
the 14 samples now carries "Reanalyzed by: GSE343291". **Their dates, and both parent Series'
dates, are Aug 12 2026, not Oct 14** (measured). The date moved when the link was made, while
the reanalysis was private. If GEO hides the back-link until the reanalysis is public, a
refresh between Aug 12 and Oct 14 saved the samples without the line, and no later date
change prompts a re-fetch (inferred: whether GEO shows the line before release was not
checked).

**SRA links.** Not tested. A sample's SRA relation line is usually in place before release
(inferred). The snapshot diff on `extrelations` would show a late one.

## 4. The second signal

| Signal | Catches | Cost |
| --- | --- | --- |
| Relation targets of Series fetched this refresh | late "Reanalyzed by", "SubSeries of" lines | none: from SOFT already fetched |
| `geo-series` snapshot, uid set diff | withdrawn Series | none: snapshot renewed anyway |
| `geo-series` snapshot, row diff | sample list, SRA link, file types, PubMed ID changes | one join of two snapshots |
| Rolling re-fetch of a slice of `filelist.txt` | GEO repacks of `_RAW.tar` | about 1/12 of file lists a month |

The first two are recommended (inferred from sections 3 and 1). The row diff is a cheap
backstop. The rolling re-fetch is optional: the only miss it fixes is an archive's time and
size, with member files unchanged.

## Sources

- E-utilities `einfo`, `esearch` and `esummary` on `db=gds`, queried 2026-11-06 (first-party
  API).
- GEO record pages (`acc.cgi`, brief SOFT) and the GEO download site folder listings, read
  2026-11-06.
- GEO, "Updating your GEO records" (`https://www.ncbi.nlm.nih.gov/geo/info/update.html`, page
  dated July 16, 2024), read through a public mirror because the live page answered with a
  robot check.
