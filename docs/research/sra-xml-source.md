# Where full-scale SRA experiment and run XML comes from

Question: should the full-scale store take its SRA experiment and run XML from per-record
Entrez `efetch` (as stage 1 does), the monthly `NCBI_SRA_Metadata_Full` tarball, or the daily
tarballs?

Evidence marks: **(M)** measured in this session, **(R)** read in a primary source,
**(I)** inferred. All measurements were taken on 2026-10-15. The scripts are in
`scripts/research/sra_xml_source/`.

## Answer

Use per-record `efetch` of `EXPERIMENT_PACKAGE` XML, the same call stage 1 makes. The
tarball's run XML is the submitter's view only. It has none of the facts SRA computes after
loading: spot and base counts, read statistics, file lists, and pool members. Stage 1's
`Run` reads those facts. Fetching every GEO-linked experiment is about 56,000 calls at 100
packages a call, a few hours with one API key.

## 1. Does the tarball hold the same facts as `EXPERIMENT_PACKAGE`?

**Method (M).** I range-read the first 300 MB of `NCBI_SRA_Metadata_Full_20261013.tar.gz`
(18,459,169,585 bytes in total, so about 1.6 %) and unpacked it as a truncated stream. It
yielded 226,279 submission folders (DRA, ERA and SRA). Of these, 2,766 have a `submission.xml`
with `center_name="GEO"`. I took 7 GEO-brokered submissions spread from the lowest to the highest SRA numbers
(`SRA008174`, `SRA452789`, `SRA999609`, `SRA1196907`, `SRA2405581`, `SRA2493140`,
`SRA2493208`). For each experiment, I fetched its package with
`efetch db=sra rettype=full retmode=xml`. Then I compared every element and attribute path,
with its values, record by record: 32 experiments, 83 runs, 28 samples and 4 studies.

**Layout (M).** A folder holds up to five files: `<SRA>.submission.xml`, `.experiment.xml`,
`.run.xml`, `.sample.xml` and `.study.xml` (plus `.analysis.xml` for a few). The tarball
member order is not sorted by accession, so there is no way to seek to one submission.
Finding a submission means streaming the whole 18 GB file.

**Experiment, sample and study: the same facts (M).** On every shared path the values were
identical (0 value differences). The differences are minor:

| Record | Only in the tarball | Only in the package |
| --- | --- | --- |
| EXPERIMENT | `IDENTIFIERS/SUBMITTER_ID` (the GSM alias; 32 of 32) | nothing |
| SAMPLE | nothing | `IDENTIFIERS/SUBMITTER_ID` (3 of 28) |
| STUDY | nothing | nothing |

`EXPERIMENT/DESIGN/LIBRARY_CONSTRUCTION_PROTOCOL`, attributes and `EXPERIMENT_LINKS` are in
both (M).

**Run: the tarball lacks everything SRA computes (M).** Every tarball run has only `IDENTIFIERS`,
`EXPERIMENT_REF` and, sometimes, `SPOT_DESCRIPTOR`. The package run adds these (count of 83 runs
in brackets):

- `RUN@total_spots`, `@total_bases`, `@size`, `@published`, `@is_public`, `@load_done`,
  `@cluster_name`, `@static_data_available` (83).
- `Statistics@nreads`, `@nspots` and `Statistics/Read@index/count/average/stdev`, the
  per-read lengths (83).
- `SRAFiles/SRAFile` (file name, size, md5, date, `semantic_name`, URLs, and
  `Alternatives` with cloud locations), and `CloudFiles` (83).
- `Bases` (base composition) (83).
- `Pool/Member` (organism, `tax_id`, spots, bases, sample name) (58). It is also a
  package-level `Pool` element.
- `Databases/Database/Table/Statistics` (79).

In return, 9 of 83 tarball runs had a `SPOT_DESCRIPTOR` that the package lacked (M).

**The package level (M).** A package is `EXPERIMENT, SUBMISSION, Organization, STUDY, SAMPLE,
[Pool,] RUN_SET`. The tarball has `submission.xml` but no `Organization` block (the submitter
contact). No file kind in the 226,279 folders holds it (M).

**Joins (M).** Newer GEO submissions do not carry their own study, and sometimes not their
samples. `SRA2405581`, `SRA2493140` and `SRA2493208` (2026) have no `study.xml`. Their package
points at a study (`SRP012412`, `SRP013565`) that sits in another submission folder. Building a
package from the tarball would need a join across folders on accession, over the whole archive.

**What stage 1 reads (R, code).** `labdata.geo.sra_records.Run` reads `RUN@total_spots`,
`@total_bases`, `@is_public` and `Statistics/Read`. `Experiment.organism` falls back on
`Pool/Member@organism`. `labdata.store.sources.sra_package` flattens whole `RUN` elements into
the store, so a tarball run would lose all the fields above.

**Partial cover from other dumps.** `SRA_Accessions.tab` has `Spots`, `Bases`, `Md5sum`,
`Status`, `Visibility`, `Published` and `ReplacedBy` per run (R, the stage 1 source survey). It has no
per-read statistics, no file list and no pool. Whether the S3 Parquet table holds them was not
checked here.

## 2. Completeness and currency

**Public only? (M)** I range-read 300 MB of `SRA_Accessions.tab` (rows from 2022). For each row
whose submission folder was in the unpacked part, I checked whether the accession appears in
that folder:

| Status, visibility | In the tarball |
| --- | --- |
| live, public | 5,449 of 5,449 experiments; 10,154 of 10,154 runs |
| live, `controlled_access` (dbGaP) | 30 of 30 experiments; 30 of 30 runs |
| suppressed | 0 of 1 experiment; 0 of 12 runs |
| unpublished | 0 of 1 experiment; 0 of 1 run; 0 of 12 samples |

So the tarball holds live records, controlled access included, and leaves out suppressed and
unpublished ones (M; the counts of suppressed records are small). Records replaced by others
were not tested.

**`efetch` drops the same records (M).** The suppressed `SRX490498` returns
`<ERROR …>SRA Experiment SRX490498 is not public</ERROR>`. The suppressed run `SRR20690131`
and the unpublished `SRX16504045` return `ID list is empty`. Neither source gives XML for a
suppressed record. `SRA_Accessions.tab` is the only place that records their status.

**Entrez counts (M).** `esearch db=sra`: `all[filter]` 47,206,983;
`public[Access]` 40,365,079; `controlled[Access]` 1,682,580; `sra_gds[Filter]` (SRA
records linked to GEO) 5,639,296, of which `public[Access]` 5,627,738.

**How current (M).** The directory listing on 2026-10-15 has 13 monthlies, from
`Full_20251017` (14 GB) to `Full_20261013` (17 GB, written 2026-10-14). A new one appears about
every 30 days, so between releases the newest is 0 to about 31 days old. Every tar member has the
same generation-day timestamp (2026-10-14). The member times say nothing about when a record
changed.

**Dailies (M).** There are 347 daily files, from 2025-09-30 to 2026-10-15, each 5.9 to 7.0 GB.
The first 300 MB of the 2026-10-15 daily holds 4,837 folders. Their median SRA number is
2,566,720 and the largest is 2,569,406. Those are newer than any in the unpacked monthly part,
whose largest is 2,566,381. Of 271 folders in both, 248 are byte-identical and 23 differ.
Folders reach 265 MB each. So a daily is a rolling set of recently created or changed
submissions, written whole, not a record-level delta (I). The dailies have the same five-file
layout, so they have the same gaps in the run XML (I).

## 3. Cost of per-record `efetch` at GEO scale

**Published limits (R).**

- With an API key, a site may send up to 10 requests per second. Without one, the limit is 3.
  NCBI can raise a key's limit on request.
  [NCBI Insights, "New API Keys for the E-utilities"](https://ncbiinsights.ncbi.nlm.nih.gov/2017/11/02/new-api-keys-for-the-e-utilities);
  [NLM support KA-05318](https://support.nlm.nih.gov/knowledgebase/article/KA-05318/en-us).
- `efetch` `retmax` is at most 10,000 records per request
  ([NLM E-utilities guide](https://dataguide.nlm.nih.gov/eutilities/utilities.html)).
  The E-utilities book (NBK25497) returned a bot challenge, so its off-peak guidance for large
  jobs was not re-read here.

**Measured calls (M).** I fetched 1,200 random UIDs from `sra_gds[Filter]` with POST, without
a key:

| Batch | Time | Bytes | Packages | Runs |
| --- | --- | --- | --- | --- |
| 100 | 2.5 s | 1.38 MB | 100 | 100 |
| 100 | 1.9 s | 1.38 MB | 100 | 100 |
| 200 | 3.3 s | 1.84 MB | 200 | 200 |
| 500 | 8.2 s | 4.61 MB | 500 | 540 |

Every requested package came back. A call costs about 16 ms per package, and a package is
9 to 14 KB.

**Estimate at scale (I, from the counts and timings above).**

- Target: about 5.64 million GEO-linked experiments (`sra_gds[Filter]`), with about 1.1 runs
  each.
- Calls: 56,400 at 100 a call, the stage 1 batch; 11,300 at 500 a call.
- Serial time: about 31 to 35 h at 100 a call, or about 26 h at 500.
- With one key: 10 requests per second is not the bound, because a call takes 2 to 8 s. The
  bound is how many calls run at once. Eight at once gives about 4 h at 100 a call, or about
  3 h at 500. Two keys, on two machines, halve that. The server's latency under load is not
  known.
- Volume: about 50 to 80 GB of uncompressed XML.

## 4. Recommendation

**Fetch the `EXPERIMENT_PACKAGE` XML per record with `efetch`, in batches of a few hundred
experiments.** Take the experiment list and every record's status from `SRA_Accessions.tab`
(GSM in `Alias`, `Center=GEO`), not from `elink`. Record records that are suppressed or
unpublished from that file, because no XML source serves them.

Cons:

- About 11,000 to 56,000 calls, and a few hours of wall time per key. The fetch needs resumable
  chunks, retries, and a check for packages that do not come back. Stage 1's
  `fetch_package_texts` already does this check.
- The run depends on NCBI's service. It is subject to throttling, outages and changes in
  latency, and a long job may be asked to run off-peak.
- A package repeats the study and sample in each experiment of a Series, so bytes on disk are
  larger than in the tarball.

Why not the monthly tarball:

- Its run XML lacks spot and base counts, read statistics, file lists and pool members.
  Stage 1 uses these, and only `efetch` serves them.
- It is unsorted, with no index. Reading 5.6 million GEO experiments means streaming all 18 GB
  and joining studies across submission folders.
- It can be up to about a month old.

Its one advantage is a single download with no rate limit, and it is a good fit for
experiment, sample and study XML only (I).

The dailies are not a source for a first full load, because they are rolling windows with the
same run gaps. They may serve stage 3 as a change signal, but `SRA_Accessions.tab`'s `Updated`
column does that per record (I).
