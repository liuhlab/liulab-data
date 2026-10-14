---
search:
  exclude: true
---

# Which GEO file format carries a study's full metadata

Measured **2026-10-14** on ten GEO Series (GSE). The question: of the family SOFT file, the
MINiML XML file, the series matrix and the E-utilities `esummary`, which carries everything a
study needs, and which is easiest to parse? A second question: is a regenerated family SOFT
file byte-identical?

Evidence classes, as in the bulk-metadata survey: **measured** means a fetch or a script was run
on 2026-10-14 and the number is its output. **Read** means an official page says it.
**Inferred** means reasoned from measured facts and not checked directly.

## Verdict

**Use the family SOFT file** (`GSExxx_family.soft.gz`). It and MINiML carry the same fields, and
on every series below their counts agree (measured). SOFT is a flat `!Key = value` line format
that a 20-line reader parses, ships as one gzip stream with a deterministic header, and keeps
each sample's fields together. MINiML needs a tar reader and an XML parser to say the same
thing. The series matrix holds the same text, but it splits a study into one file per platform,
repeats the series header in each, and lays characteristics out by position, so one row mixes
different keys. `esummary` lacks the overall design, the contributors, every sample field and
the SuperSeries links, so it is a change feed and an index, not a metadata source.

A family SOFT file served twice is byte-identical, and a fresh render from GEO's database
matched a two-month-old FTP file line for line (measured). Hash the **decompressed** text to
detect change; the gzip bytes should also be stable, but that rests on GEO's compressor never
changing (inferred).

## The ten series

| GSE | Shape | Samples | Platforms | Species |
| --- | --- | ---: | ---: | --- |
| GSE60450 | bulk RNA-seq | 12 | 1 | mouse |
| GSE131907 | 10x single-cell RNA-seq | 58 | 1 | human |
| GSE164378 | CITE-seq (RNA plus surface protein), two series types | 9 | 1 | human |
| GSE185224 | 10x single-cell RNA-seq, intestine | 6 | 1 | human |
| GSE207086 | SuperSeries of GSE207083–GSE207085 | 1,448 | 2 | human, mouse |
| GSE207085 | SubSeries of GSE207086, Smart-seq | 1,440 | 1 | mouse |
| GSE30352 | multi-species bulk RNA-seq | 131 | 10 | 10 species |
| GSE47753 | ATAC-seq | 13 | 1 | human |
| GSE29611 | ENCODE ChIP-seq | 296 | 3 | human |
| GSE1000 | microarray, for contrast | 10 | 1 | human |

All counts are measured, and all four sources agree on the sample count for every series.

## Field coverage

Rows are what a study needs. "Yes" means every value in the SOFT file is present in that format
for all ten series (measured by a script that compared counts per field and, for SOFT against
the matrix, the exact values per sample).

| Field | Family SOFT | MINiML | Series matrix | esummary |
| --- | --- | --- | --- | --- |
| Title, summary | yes | yes | yes | yes |
| Overall design | yes | yes | yes | **no** |
| Series type (assay class) | yes | yes | yes | yes (`gdstype`) |
| Contributors, ordered | yes | yes (`position` attribute) | yes, repeated per platform file | **no** |
| PubMed IDs | yes | yes | yes, repeated per platform file | yes |
| Dates (submitted, updated) | yes | yes | yes | release date only (`pdat`) |
| SuperSeries / SubSeries links | yes | yes | yes | **no** (`relations` empty) |
| BioProject | yes | yes | yes | yes |
| Series SRA study (SRP) | when GEO lists it | same | same | when GEO lists it (`extrelations`) |
| Series supplementary files | full URLs | full URLs, typed | full URLs, repeated per platform file | file types only (`suppfile`) |
| Sample title, source, organism, taxid | yes | yes | yes | title only (`samples[]`) |
| Sample characteristics | `key: value` strings | `tag` attribute plus value | positional rows | **no** |
| Protocols (extract, growth, treatment), data processing | yes | yes | yes | **no** |
| Library strategy, source, selection; instrument | yes | yes | yes | **no** |
| Sample BioSample and SRA (SRX) links | yes | yes | yes | **no** |
| Sample supplementary files | yes | yes | yes | **no** |
| Sample's other series (`Sample_series_id`) | yes | no field | **no** | no |
| Platform title, technology | yes | yes | **no** | title only |

Details behind the cells:

- **SOFT against matrix (measured).** After undoing the matrix's CSV quote doubling (`""` for
  `"`), every sample field value matched SOFT exactly on all ten series, except that the matrix
  has no `Sample_series_id`. For GSE29611 that field shows its samples also belong to GSE51334,
  which the matrix cannot tell you.
- **SOFT against MINiML (measured).** The counts of samples, platforms, series relations,
  PubMed IDs, contributors, series and sample supplementary files, characteristics and sample
  relations agree on every series. One exception: for the 2004 microarray series GSE1000, MINiML
  writes a placeholder characteristic `none` per sample where SOFT writes nothing.
- **SuperSeries (measured).** The family SOFT of GSE207086 holds all 1,448 samples of its three
  SubSeries and lists `SuperSeries of: GSE207083/4/5`. It has **no** contributors of its own; the
  names sit on the SubSeries. GSE207085's file says `SubSeries of: GSE207086`. In `esummary`
  both series have `relations: []`, as the survey found for GSE207085. A crawler that wants a
  SuperSeries's authors must follow the SubSeries links.
- **SRA links (measured).** No format carries a run (SRR) or the library layout; the survey
  already showed these come from SRA. A series-level `SRA:` relation is missing from GSE131907,
  GSE164378, GSE207085 and GSE207086 in every format, so the sample-level `SRA: …term=SRX…`
  relation is the reliable join key.
- **esummary (measured).** Its fields are `accession`, `bioproject`, `entrytype`,
  `extrelations`, `ftplink`, `gds`, `gdstype`, `geo2r`, `gpl`, `gse`, `n_samples`, `pdat`,
  `platformtaxa`, `platformtitle`, `projects`, `ptechtype`, `pubmedids`, `relations`,
  `samples`, `samplestaxa`, `seriestitle`, `ssinfo`, `subsetinfo`, `summary`, `suppfile`,
  `taxon`, `title`, `uid` and `valtype`. One call fetched all ten records.

## Parsing cost

- **Family SOFT.** One gzip stream. Lines start with `^` (a new entity: `SERIES`, `SAMPLE`,
  `PLATFORM`) or `!` (`Key = value`, repeated keys allowed). Characteristics are `key: value`
  strings, so split on the first `": "`; the key is the submitter's free text. Microarray
  families embed data tables between `!sample_table_begin` and `!sample_table_end` (and the
  platform's annotation table), so the reader must skip those blocks: GSE1000's SOFT is 9.4 MB
  compressed and 53 MB raw for ten samples (measured), against 39 KB raw for GSE60450.
- **MINiML.** A `.tgz` holding `GSExxx_family.xml`; for microarray series it also holds one
  `-tbl-1.txt` data file per sample and platform (measured on GSE1000). The XML is namespaced and
  uses ID references (`Contributor-Ref`, `Platform-Ref`), and long text sits between newlines
  that need stripping. It gives the characteristic key as a `tag` attribute, which saves one
  string split; that is its only parsing advantage over SOFT. The README says SOFT and MINiML
  "generally contain the same data" (read, `https://ftp.ncbi.nlm.nih.gov/geo/README.txt`).
- **Series matrix.** One file per platform for multi-platform series (10 files for GSE30352,
  3 for GSE29611; measured), each repeating every series line, so contributors and PubMed IDs
  must be deduplicated (GSE30352: 180 contributor lines for 18 names). Sample fields are rows
  with one column per sample, CSV-quoted. Characteristics rows are positional: in GSE29611, 73 of
  76 characteristics rows mix more than one key across samples, and GSE47753 has 2 of 3
  (measured). The reader must therefore re-key each cell from its own `key:` prefix. The matrix
  also drops the platform section and `Sample_series_id`.
- **esummary.** JSON, the easiest to parse, but too thin (see the table).

## Is a regenerated SOFT file byte-identical

- **Two fetches, 20 s apart (measured).** For all ten series the SHA-256 of the `.gz` and of
  its decompressed text matched between fetches. The FTP `Last-Modified` dates ranged from
  2026-08-10 to 2026-10-05, so this compares one stored file with itself; it does not exercise
  regeneration.
- **Gzip header (measured).** All ten `.gz` files have header flags 0 (no stored file name) and
  MTIME 0. Recompressing unchanged text with the same compressor therefore yields the same
  bytes (inferred).
- **Fresh render against the stored file (measured).** GEO's on-demand SOFT view,
  `https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE60450&targ=all&form=text&view=full`,
  rendered the record from the database on 2026-10-14. Two renders were byte-identical. Its
  `SERIES` and `SAMPLE` blocks matched the FTP file written 2026-08-10 line for line, except for
  two lines the web view adds (`!Series_platform_organism`, `!Series_sample_organism`). The
  web view lacks the `^DATABASE` header and embeds the platform's list of every GSM on that
  platform (205,246 lines for GPL13112), so it is not a drop-in for the FTP file.
- **What this means for refresh.** For an unchanged record, GEO's renderer gives the same text
  two months apart, so a hash of the decompressed family SOFT is a sound change detector
  (inferred from the fresh-render match). A MINiML `.tgz` is less stable: its tar member stores
  the regeneration time and owner (measured: `geoadm geo … Aug 10 02:27` for GSE60450), so its
  bytes change on every regeneration even when the XML does not (inferred).

## Method

- Files came from `https://ftp.ncbi.nlm.nih.gov/geo/series/GSEnnn/GSExxx/{soft,miniml,matrix}/`
  and `esummary.fcgi?db=gds&retmode=json&id=…` (GSE uid = 200,000,000 + the number). No API key
  was set; about 100 requests in total, with pauses.
- A script parsed each format and compared per-field counts, and per-sample values for SOFT
  against the matrix. Downloaded files are not committed.
- GSE2034 was dropped as the microarray example because its family SOFT is 65 MB compressed;
  GSE1000 (9.4 MB) stands in.
