# How a refresh learns a record is gone

Question: for each record type the store holds (GEO Series and sample, SRA experiment and run,
BioSample, BioProject), which source marks it withdrawn, suppressed, replaced or private
again? Does comparing two dated snapshots catch every case? What does a removed record look
like at its source, so a refresh can tell "gone" from "failed to fetch"?

Evidence marks: **(M)** measured in this session, **(R)** read in a primary source,
**(I)** inferred. All probes ran on 2026-11-06 against the live NCBI services: range reads of
the bulk files (at most 40 MB each) and a few dozen single E-utilities and GEO `acc.cgi`
calls, spaced at least half a second apart.

## Answer

No single snapshot diff catches every case, but every record type has one signal that does,
and every "gone" answer arrives as HTTP 200 with a record-level message. A failed fetch is a
non-200 status, a timeout or malformed XML. One trap: SRA `efetch` answers HTTP 400 when
*every* accession in the batch is non-public or unknown.

| Record | Where removal shows | Snapshot diff catches it? | "Gone" at the source |
| --- | --- | --- | --- |
| GEO Series | drops out of the `gds` list | yes, as absence (deleted and private alike) | `acc.cgi`: "was deleted by the GEO staff on …" or "is currently private" |
| GEO sample | drops out of its Series SOFT (I) | only through its Series | `acc.cgi`, same messages (I) |
| SRA experiment, run | `Status` in `SRA_Accessions.tab` | yes, if diffed by accession, not by GEO filter | `efetch`: `<ERROR>… is not public</ERROR>` in a 200 body |
| BioSample | `<Status status="suppressed">` in the bulk XML; withdrawn ones vanish | yes: status change or absence | `efetch`: empty `<BioSampleSet>` |
| BioProject | absent from the bulk XML (public only) | yes, as absence | `efetch`: `<error>The following ID is not public in BioProject</error>` |

## 1. What NCBI says the statuses mean

**SRA and GenBank (R).** NCBI's sequence data processing page defines two removal states.
Suppressed data "were previously public", "have been removed from the NCBI text-based search
and comparative analysis results" and "may be accessed only by accession number". Withdrawn
data are removed the same way and "cannot be accessed by the public even by accession number".
For duplicates, the older record is either suppressed or kept as a secondary accession on the
newer one ([sequence data processing](https://www.ncbi.nlm.nih.gov/sra/docs/sequence-data-processing/)).

**GEO (R).** "Only GEO staff can remove records from the database." If the accession has been
cited in a manuscript or preprint, GEO does not delete it: "a comment will be added to the
record indicating the reason the submitter requested withdrawal of the data, and the record
content will be adjusted/deleted accordingly." SRA records under a GEO submission follow the
SRA procedures above (GEO FAQ, "How can I delete my records?",
[faq.html](https://www.ncbi.nlm.nih.gov/geo/info/faq.html); read through a verbatim mirror,
because the NCBI site served a browser check to this machine). A cited record cannot stay
private: GEO staff must release it (same FAQ).

**BioProject (R).** The FTP README calls `bioproject.xml` the "dump of all publically
available projects", updated daily
([README](https://ftp.ncbi.nlm.nih.gov/bioproject/README)).

## 2. SRA: the `SRA_Accessions.tab` status column

**Values (M).** In four slices of the tab (about 295,000 rows from the start, 10 GB, 25 GB
and 34 GB offsets), `Status` took three values: `live`, `suppressed` and `unpublished`.
`withdrawn` never appeared. `Visibility` was `public` or `controlled_access`. A
`ReplacedBy` column names the replacing accession for a few suppressed rows, for example
`SRP071357` replaced by `DRP002864`.

**Non-live rows lose their links (M).** Every `suppressed` and `unpublished` row of type
RUN, EXPERIMENT, SAMPLE or STUDY had `-` in `Center`, `Alias`, `BioSample` and `BioProject`.
Every `live` row of those types had them filled. So when a GEO experiment is suppressed, its
row stays in the tab but no longer says `Center=GEO` or carries its GSM alias. A refresh that
selects GEO experiments with that filter (half of the current E-utilities lane's list; the
other half comes from saved SOFT) sees the experiment vanish, not change status. Diff the tab by accession instead: an experiment the
store holds whose row now reads `suppressed` or `unpublished` is gone.

**Does a row ever leave the tab? (unresolved)** The tab is ordered by submission, not by
accession, so a range read cannot show gaps. Whether a withdrawn record keeps a row needs a
full-file diff of two dated tabs, which the store already holds.

**Entrez view of a suppressed experiment (M).** For `SRX27172985` and `SRX022015` (both
`suppressed` in the tab):

- `esearch db=sra` still finds them (count 1).
- `esummary` reports `<Experiment … status="withdrawn"/>` with an empty run list, so Entrez
  and the tab name the same state differently. A live one reports `is_public="true"`.
- `efetch rettype=full` returns HTTP 200 with
  `<ERROR … >SRA Experiment SRX27172985 is not public</ERROR>` inside
  `EXPERIMENT_PACKAGE_SET`. In a batch with a live experiment, the live package comes back
  and the error element sits beside it.

Despite the "accessible by accession" definition, the package of a suppressed experiment is
not served (M, two accessions).

**Unpublished and unknown accessions (M).** For `SRX27172302` (`unpublished`), a suppressed
run `SRR31812539`, and a made-up `SRX99999999999`, `esearch` returns count 0 and `efetch`
alone returns **HTTP 400** with `ID list is empty! In it there are neither IDs nor
accessions.` In a batch with a live accession, the unknown one is dropped silently and the
call returns 200 with only the live package.

## 3. GEO Series and samples

**The `gds` list (M).** `esearch db=gds term=<GSE>[ACCN]` returns count 0 for a deleted
Series (`GSE250011`, `GSE100030`) and for a private one (`GSE250010`). `esummary` on their
UIDs gives `"error":"cannot get document summary"` with HTTP 200. The GEO FTP folder of both
returns 404. So a Series that was in the last `geo-series` snapshot and is missing from the
new one is either deleted or private again; the diff alone cannot say which.

**`acc.cgi` tells them apart (M).** `acc.cgi?acc=<GSE>&targ=self&form=text&view=brief`
returns HTTP 200 in every case. A public record starts with SOFT lines
(`!Series_status = Public on <date>`). Otherwise it returns an HTML page whose message is
one of:

- `Accession "GSE250011" was deleted by the GEO staff on Sep 16, 2024.`
- `Accession "GSE250010" is currently private and is scheduled to be released on <date>.`
- `Could not find a public or private accession "GSE999999999"`

In 20 consecutive Series from `GSE250000`, 5 were private and 1 deleted; in 11 spread over
`GSE100005`–`GSE100099`, 1 was deleted (M). GSM pages give the same private message
(`GSM7970000`, M). I found no deleted GSM in 38 probes, so its message is inferred to match
the Series one (I).

**Withdrawn but cited (I).** A cited Series that a submitter withdraws stays public with a
comment and reduced content (R, above). It stays in `gds`, so only change detection (a newer
update date and different SOFT) catches it, the same path as any edit.

**Private again (I).** GEO's FAQ forbids keeping cited records private, and the only route
back to private is staff action. The diff treats it like a deletion, which is the right
outcome for a current-state mirror.

**Samples (I).** The store has no bulk sample list; samples come from their Series' SOFT. A
sample removed from a public Series drops out of the next SOFT, so a Series refetch catches
it. A refresh only refetches changed Series, so it relies on the sample removal also
updating the Series date.

## 4. BioSample

**The bulk XML keeps suppressed records (M).** The first 40 MB of `biosample_set.xml.gz`
(533,089 records, `SAMN00000002`–`SAMN00569416`) held 424,644 `<Status status="live">` and
107,867 `status="suppressed"`, no other value. About 36,000 numbers in that range are absent,
for example `SAMN00000082`.

**Entrez (M).** `efetch` of suppressed `SAMN00004907` returns the full record with
`<Status status="suppressed" when="2012-08-02…"/>`. `efetch` of absent ID `82` and of a
made-up `SAMN99999999999` both return HTTP 200 with an empty `<BioSampleSet></BioSampleSet>`;
`esummary` of `82` returns `cannot get document summary`. An absent BioSample cannot be told
apart from a never-issued number, which is fine: both are "not found".

`all[filter] NOT public[filter]` matches 13.4 million BioSamples, but the ones checked were
`access="controlled-access"` and `live`, not suppressed (M). Controlled access is not removal.

## 5. BioProject

**The bulk XML is public only (R, M).** In the first 30 MB of `bioproject.xml`, 403 of 403
packages have `<Access>public`. Numbers between them are missing (`PRJNA6`, `PRJNA10`,
`PRJNA14`).

**Entrez (M).** `efetch db=bioproject id=6` and a made-up `id=999999999` both return HTTP
200 with `<DocumentSummary uid="6"><error>The following ID is not public in BioProject:
6</error>`; in a batch, the public record and the error sit side by side. `esearch PRJNA6`
returns count 0.

## 6. Telling "gone" from "failed to fetch"

| Response | Means |
| --- | --- |
| HTTP 200, record present | live |
| HTTP 200, record-level `<ERROR>`/`<error>`, empty set, or `cannot get document summary` | gone |
| HTTP 200, `acc.cgi` HTML with "deleted", "private" or "Could not find" | gone |
| SRA `efetch` HTTP 400 "ID list is empty" for a batch of only non-public IDs | gone (I: no other case seen) |
| HTTP 429, 5xx, timeout, truncated or malformed XML | failed: retry |

A batch that returns fewer records than it asked for, with no error element, means the
missing ones are not public (M for SRA). The safest rule for a refresh (I): decide "gone" from
the snapshot signal (tab status, absence from the bulk XML or the `gds` list), then confirm
each one with a single-accession call whose 200 response states it. Never decide "gone" from
a batch that failed.

## Open

- Whether a withdrawn SRA record keeps a row in `SRA_Accessions.tab` or leaves it: diff two
  dated tabs the store already holds.
- The GSM deletion message: not found in 38 probes.
