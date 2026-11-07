# Can the SRA tab's status mark a record gone?

Question: can the `Status` column of `SRA_Accessions.tab` be trusted to mark an SRA experiment
or run as gone? May a refresh remove a held record on the tab's status alone, or must it
confirm with a fetch first, and for which statuses?

Evidence marks: **(M)** measured in this session, **(R)** read in a primary source,
**(I)** inferred. The tab is NCBI's file with Last-Modified 6 Nov 2026, streamed once on
2026-11-06. The live checks ran on 2026-11-06 against E-utilities, one request at a time,
at least 0.45 s apart, no API key.

## Answer

The tab's `suppressed` and `withdrawn` are right; its `unpublished` is not.

| Tab status | Sampled | Live and served | Not served | Rule for a refresh |
| --- | ---: | ---: | ---: | --- |
| `unpublished`, Published date 12 Oct – 6 Nov | 70 | **61** | 9 | never act on it: confirm with a fetch |
| `unpublished`, Published date before 12 Oct | 93 | 0 | 93 | confirm with a fetch (same rule, one status) |
| `suppressed` | 60 | 0 | 60 | may act on it; a confirming fetch is cheap |
| `withdrawn` | 52 | 0 | 52 | may act on it; a confirming fetch is cheap |
| `live` (control) | 30 | 30 | 0 | — |

(M, all rows.) So: **a refresh must not remove a held record because the tab says
`unpublished`.** Most recent `unpublished` rows are public records the tab has not caught up
with. For `suppressed` and `withdrawn`, none of 112 sampled rows was still served, so the
status alone is sound evidence (an error rate above about 3% would likely have shown, by the
rule of three) (I). The simplest safe rule is one rule for every non-live status: confirm it
with a single-accession fetch whose 200 response says the record is not public (I).

The tab also misses removals: since mid-October it stopped updating old rows (see the
answer on GEO-linked SRA changes), so a record suppressed after that shows as `live`. The tab
can trigger a check; it cannot be the only source of "gone" (I).

## 1. What the tab holds

Experiment and run rows by `Status` in the 6 Nov file (M):

| Status | Experiments | Runs |
| --- | ---: | ---: |
| `live` | 42,225,217 | 44,927,386 |
| `unpublished` | 4,433,079 | 4,908,616 |
| `suppressed` | 1,152,414 | 3,749,139 |
| `withdrawn` | 32 | 2,639 |

`unpublished` rows whose `Published` date has passed, by Published month (experiments) (M):
a few thousand to 87,871 a year from 2015 to 2025, about 4,000–10,500 a month in January to
August 2026, then 22,544 in September, 106,159 in October and 31,899 in the first six days
of November. The jump starts in mid-October, when the tab stopped turning releases live.

## 2. Strata and results

Each stratum is a seeded random sample of the tab's rows (reservoir sampling over the whole
file). A record counts as **live** when `efetch` returns its experiment package with the
experiment, or the run, inside it. The `esummary` status is NCBI's own status for the same
record (M for every cell).

| Stratum (rows in tab, exp / run) | Sample | efetch | esummary status |
| --- | --- | --- | --- |
| A. `unpublished`, Published 12 Oct – 6 Nov (136,165 / 150,527) | 50 exp, 20 run | 45 exp and 16 runs served; 8 "is not public"; 1 HTTP 400 | public 45; `hup` 5 |
| B. `unpublished`, Published before 12 Oct (558,040 / 687,279) | 50 exp, 20 run; Published 2011–2026 | 0 served; 48 "is not public"; 21 HTTP 400; 1 run left out of its public experiment | `suppressed` 45, `hup` 3, no record 2 |
| B2. same, Published 1 Aug – 11 Oct 2026 | 23 exp | 0 served; 22 "is not public"; 1 HTTP 400 | `suppressed` 21, `withdrawn` 1 |
| C. `suppressed` (1,152,414 / 3,749,139) | 40 exp, 20 run | 0 served; 21 "is not public"; 26 HTTP 400; 3 "TCGA sequence data has moved to CGHub"; 10 runs left out of their public experiment | `withdrawn` 40 of 40 experiments |
| D. `withdrawn` (32 / 2,639) | 32 exp, 20 run | 0 served; 32 "is not public"; 19 runs left out of their public experiment; 1 HTTP 400 | `withdrawn` 32 of 32 |
| E. `live` | 20 exp, 10 run | 30 served | public |
| F. `unpublished`, Published after 6 Nov (control) | 10 exp | 0 served; 10 "is not public" | `hup` 10 |
| G. `unpublished`, no Published date | 10 exp | 1 served; 9 not | `completed` 5, `suppressed` 1, no record 3 |

Notes (M unless marked):

- **Stratum A is general SRA, not GEO.** Only 4 of the 45 served experiments carry a GSM
  alias. The tab's lag after mid-October covers every submitter, not GEO alone. This corrects
  the guess (I) in the answer on GEO-linked SRA changes that these rows are GEO releases.
- **The tab and Entrez use different words for the same state.** Old `unpublished` rows are
  `suppressed` in `esummary`; tab `suppressed` rows are `withdrawn` in `esummary`. `hup`
  ("hold until published") is a record still private. Map tab statuses to "served or not",
  never to NCBI's status names one to one.
- **A gone run has no message of its own.** `efetch` on a suppressed or withdrawn run
  accession returns HTTP 200 with its experiment's package, and that package simply leaves
  the run out (29 of 40 such runs). The confirming check for a run is "the run accession is
  inside the returned package", not "the response has an error".
- **HTTP 400 "ID list is empty"** came back for 50 of the 214 non-served records in strata A–D. For a
  single accession it means not public or not known; it is never proof the record exists.
  Never send it as part of a mixed batch: the batch call silently drops those IDs.

## 3. What a confirming fetch should accept as "gone"

From the responses above (M) and the earlier note on removed records (R, the
`withdrawn-records` research note):

1. One accession per call, `efetch db=sra id=<accession>`.
2. **Gone:** HTTP 200 with `<ERROR …>SRA Experiment … is not public</ERROR>` or a
   `<Message>` instead of a package; HTTP 200 with a package that does not list the run;
   or HTTP 400 "ID list is empty" for that one accession.
3. **Live:** HTTP 200 with a package that holds the experiment or run.
4. **Failed, keep the record:** any other status, a timeout, or XML that does not parse.

## Gaps

- No record was sampled from the 2 Nov – 6 Nov week alone; stratum A pools 12 Oct – 6 Nov.
- Whether NCBI repairs the tab later was not tested; a later file would show it.
- The samples bound the error rate only roughly: 0 of 112 means "rarely wrong", not "never".

## Method

Extraction, one pass over the streamed file inside the held dev job (only accession,
status, dates, type, visibility, experiment and `ReplacedBy` kept):

```awk
BEGIN { FS = "\t"; OFS = "\t"; srand(113) }
NR == 1 { next }
$7 != "EXPERIMENT" && $7 != "RUN" { next }
$3 != "live" { print $1, $3, $4, $5, $6, $7, $9, $11, $20; n[$7 OFS $3]++; next }
{ n[$7 OFS $3]++; if (rand() < 0.00002) print $1, $3, $4, $5, $6, $7, $9, $11, $20 > "live_sample.tsv" }
```

Strata were a reservoir sample of 60 rows per stratum and type (seed 113), cut to the
sizes above. Each accession then got, in order: `efetch db=sra id=<acc>`;
`esearch db=sra term=<acc>[ACCN]`; `esummary db=sra id=<uids>`, reading `status` from
`<Experiment … status=…>` and `is_public` from `<Run …>`. An earlier batched `efetch` pass
(20 IDs a call) gave the same live/not-live split.
