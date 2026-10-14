---
search:
  exclude: true
---

# Which source carries each study, sample, experiment and run field

Question: for each field a study needs, which source carries it? Where two sources carry the
same field, which one should win? And how much does the join from a GEO sample (GSM) to an SRA
experiment (SRX) lose?

All checks ran on 2026-10-14 against live NCBI services and the bulk files dated 2026-10-14.
Only metadata was read. Each table row has a mark:

- **measured**: a live probe or a bulk query printed it on 2026-10-14.
- **read**: an official page, a schema or an earlier note says it.
- **to-verify**: needs the bulk GEO family SOFT files, which were still downloading.

## Answer

- **GEO wins on text, SRA wins on reads.** For a study with a GEO Series, the family SOFT file
  is the source for every text field. That covers titles, summary, design, characteristics,
  protocols, PubMed IDs, dates and SuperSeries links. SRA is the source for library layout,
  runs, spot and base counts and submitted files. GEO lacks all of these.
- **The library fields agree.** GEO and SRA agree on library strategy, source, selection and
  instrument for 60 to 63 of 63 sampled samples. Every disagreement is a pre-2011 or reused
  third-party deposit, where the SRA record describes the reads. So SRA wins.
- **For an SRA-only study, BioProject and BioSample carry the text.** The SRA `STUDY` title
  and abstract copy the BioProject. BioSample holds the sample attributes, with NCBI's
  `harmonized_name` beside each one.
- **BioSample is an annotation layer, not a copy of GEO.** Only 233 of 325 sampled GEO
  characteristics (72%) reach BioSample with the same key and value. BioSample can be a stale
  snapshot, can belong to another submitter, and rewrites values (`None` becomes `missing`,
  `Male` becomes `male`). Keep the GEO text, and take only `harmonized_name` from BioSample.
- **The bulk files match the live services.** Bulk `bioproject.xml` and
  `biosample_set.xml.gz` records equal the `efetch` replies field for field. The Parquet and
  `SRA_Accessions.tab` agree with the SRA XML on every compared field.
- **The Parquet is not complete.** It lacks 971,528 of the 43,022,633 live public runs in
  `SRA_Accessions.tab` (2.3%), and it holds no suppressed or withdrawn runs. Use
  `SRA_Accessions.tab` for the list of runs and for exact spot and base counts.
- **The join.** Of 72 random sequencing GSMs, 9 (12.5%) have no SRX. The other 63 each map to
  exactly one SRX. The SOFT `Sample_relation = SRA:` line finds all 63. In bulk,
  `SRA_Accessions.tab` aliases find 60. A GSM can map to several SRX: the sample had none, but
  GSE20301 and GSE19456 have them, and so do 450 of 5.6 million GSMs in the bulk aliases.

## Field table

Source names used below:

- **SOFT**: the GEO family SOFT file, the format chosen in
  [the GEO format note](https://github.com/liuhlab/liulab-data/issues/21).
- **SRA XML**: `efetch db=sra`, one `EXPERIMENT_PACKAGE` per experiment.
- **Parquet**: NCBI's SRA metadata table (`sra.metadata`), one row per run.
- **Accessions**: `SRA_Accessions.tab`, one row per SRA object.
- **BioProject** and **BioSample**: the bulk `bioproject.xml` and `biosample_set.xml.gz`, which
  equal the `efetch` replies.
- **esummary**: `esummary db=gds` or `db=sra`.

The seqforge column says what seqforge's `fetch_records` reads **today** and what it wants
**next**, from its list on issue #23. A dash means seqforge has not asked for the field.

### Study

| Field | seqforge | Sources | Winner and why | Mark |
| --- | --- | --- | --- | --- |
| Study ID | today | GSE (SOFT). BioProject: SOFT `Series_relation`, SRA XML `STUDY` external ID, Parquet `bioproject`, Accessions `BioProject`. SRP: SRA XML, Parquet `sra_study`, Accessions `Study` | GSE when one exists, else BioProject. The SRP is SRA's internal ID; GEO omits it from 4 of 10 Series (#21). The Parquet had a BioProject for 1 run where the SRA XML had none | measured |
| Title | today, next | SOFT `Series_title`, SRA XML `STUDY_TITLE`, BioProject `Title` | SOFT for GEO; BioProject otherwise. The other two are copies on GSE60450 and PRJNA1027859. The Parquet has no title | measured |
| Summary or abstract | today, next | SOFT `Series_summary`, SRA XML `STUDY_ABSTRACT`, BioProject `Description` | SOFT; BioProject otherwise. For GEO deposits SRA's abstract is the summary plus `Overall design:` and the design text, joined | measured |
| Overall design | next | SOFT `Series_overall_design` | SOFT | measured |
| Study type | today | SOFT `Series_type`, BioProject `ProjectDataTypeSet`, SRA XML `STUDY_TYPE` | SOFT for GEO, BioProject otherwise. The vocabularies differ, so keep both | measured |
| Contributors | – | SOFT `Series_contributor` only | SOFT. A SuperSeries has none of its own (#21) | read |
| Center | today | SRA XML `SUBMISSION center_name`, Parquet `center_name`, Accessions `Center`, BioProject owner | BioProject owner or SOFT contact institute. For every GEO deposit SRA says `GEO`, which names the broker, not the lab | measured |
| PubMed IDs | next | SOFT `Series_pubmed_id`, esummary `pubmedids`, BioProject `Publication`, SRA XML `STUDY_LINKS`, PubMed's `[si]` field | Keep each with its source. All four agree on GSE60450. For SRA-only studies NCBI rarely records one (#22) | measured |
| Dates | today | SOFT submission, last update, public date. BioProject `submitted` and `last_update`. esummary `pdat` | SOFT for GEO, BioProject otherwise | measured |
| Series relations | next | SOFT `Series_relation`: SuperSeries, SubSeries, BioProject, SRA | SOFT; esummary leaves the SuperSeries links empty (#21) | measured |
| Organisms | – | SOFT `Series_sample_taxid`, BioProject target organism | Roll up from the samples | measured |

### Sample

| Field | seqforge | Sources | Winner and why | Mark |
| --- | --- | --- | --- | --- |
| Sample ID | today | GSM (SOFT). BioSample: SOFT `Sample_relation`, SRA XML `SAMPLE` external ID, Parquet `biosample`, Accessions `BioSample`. SRS: SRA XML, Parquet `sample_acc` | GSM, plus the BioSample from the SOFT line. 7 of 63 GSMs point at a different BioSample than their SRA sample. These are 2008 to 2010 deposits that got two BioSamples. Keep both | measured |
| Sample title | today, next | SOFT `Sample_title`, SRA XML `SAMPLE/TITLE`, BioSample `Title` | SOFT. For GEO the other two copy it. For SRA-only, SRA has no title and BioSample's title is generic ("Model organism or animal sample from …"), so use the alias | measured |
| Sample alias | today | SRA XML `SAMPLE alias`, BioSample "Sample name" ID, Parquet `sample_name` | SRA XML; for GEO it is the GSM. Parquet equal for 90 of 90 runs | measured |
| Taxid | today | SOFT `Sample_taxid_ch1`, SRA XML `TAXON_ID`, BioSample `Organism` | SOFT for GEO. 2 of 63 differ, and SRA has the more specific strain-level taxid each time. Keep both | measured |
| Organism name | next | SOFT `Sample_organism_ch1`, Parquet `organism` | SOFT | measured |
| Characteristics | today, next | SOFT `Sample_characteristics_ch1` (`key: value` text), BioSample `Attribute`, SRA XML `SAMPLE_ATTRIBUTE`, Parquet `attributes` | SOFT for GEO, kept raw line by line. BioSample for SRA-only. See the join section for the loss | measured |
| Harmonized names | today | BioSample `harmonized_name` only | BioSample. The Parquet keys are the submitter's names, lowercased, with `_sam` added, so they carry no harmonized name | measured |
| Owner | today | BioSample `Owner/Name` | BioSample | measured |
| Package | today | BioSample `Package` | BioSample | measured |
| Source name | next | SOFT `Sample_source_name_ch1`, BioSample attribute `source_name` | SOFT | measured |
| Molecule | next | SOFT `Sample_molecule_ch1` only | SOFT | measured |
| Extract protocol | next | SOFT `Sample_extract_protocol_ch1`; SRA XML `LIBRARY_CONSTRUCTION_PROTOCOL` copies it for GEO deposits | SOFT | measured |
| Description | next | SOFT `Sample_description` only | SOFT | measured |
| Data processing | next | SOFT `Sample_data_processing` only | SOFT | measured |
| Sample relations | next | SOFT `Sample_relation`: SRA, BioSample, reanalysis, annotation | SOFT. See the relation check below | measured |
| Sample files | next | SOFT `Sample_supplementary_file_*` | SOFT | read |
| Other Series of the sample | – | SOFT `Sample_series_id` | SOFT | read |

### Experiment

| Field | seqforge | Sources | Winner and why | Mark |
| --- | --- | --- | --- | --- |
| Experiment ID and alias | today | SRA XML, Parquet `experiment`, Accessions `Alias` | SRA XML. GEO aliases take four forms: `GSMn`, `GSMn: title`, `GSMn_k` and `GSMn_rk` | measured |
| Experiment title | today | SRA XML `TITLE` | For GEO it is generated: "GSM…: sample title; organism; strategy" | measured |
| Library strategy, source, selection | today, next | SRA XML, SOFT `Sample_library_*`, Parquet `assay_type`, `librarysource`, `libraryselection` | SRA. SOFT agrees for 61, 63 and 60 of 63 GSMs, ignoring case; every mismatch is an old or reused deposit. Parquet equals the SRA XML for 90 of 90 runs | measured |
| Library layout | next | SRA XML `LIBRARY_LAYOUT`, esummary sra, Parquet `librarylayout` | SRA XML. GEO has no layout. Parquet equal for 90 of 90 | measured |
| Nominal length | next | SRA XML `PAIRED@NOMINAL_LENGTH`, Parquet `insertsize` | SRA XML, but it is rare: 1 of 33 paired runs sampled. The Parquet `insertsize` was 0 for all 90 runs, that one included | measured |
| Library name | today | SRA XML `LIBRARY_NAME`, Parquet `library_name` | SRA. Often empty for GEO deposits. Parquet equal for 90 of 90 | measured |
| Instrument | today | SRA XML, SOFT `Sample_instrument_model`, Parquet `instrument` | SRA. SOFT agrees for 62 of 63. Parquet equal for 90 of 90 | measured |
| Design description | today | SRA XML `DESIGN_DESCRIPTION` | SRA. Empty for GEO deposits | measured |
| Library construction protocol | today | SRA XML `LIBRARY_CONSTRUCTION_PROTOCOL` | SRA for SRA-only; for GEO it copies the SOFT extract protocol. Not in the Parquet | measured |
| GEO platform (GPL) | – | SOFT only | SOFT | read |

### Run

| Field | seqforge | Sources | Winner and why | Mark |
| --- | --- | --- | --- | --- |
| Run ID and alias | today | SRA XML `RUN`, Parquet `acc`, Accessions | Accessions for the full list: the Parquet lacks 2.3% of live public runs. SRA XML for one study | measured |
| Spots and bases | next | SRA XML `total_spots`, `total_bases`; esummary sra; Accessions `Spots`, `Bases`; Parquet `mbases`, `avgspotlen` | Accessions: it equals the SRA XML for 91 of 91 runs. The Parquet rounds down to whole megabases and whole bases per spot, so it cannot give an exact spot count | measured |
| Read lengths | – | SRA XML `Statistics/Read` only | SRA XML. PRJNA1027859 shows 28 + 94 bases, the 10x read layout | measured |
| Size in bytes | – | SRA XML `size`, esummary, Parquet `mbytes` | SRA XML | measured |
| Submitted files | today | SRA XML `SRAFile supertype="Original"`: name, md5, size, first URL | SRA XML only; no bulk file lists them | measured |
| Release date | – | SRA XML `published`, Parquet `releasedate`, Accessions `Published` | Accessions. Parquet matches the SRA XML day for 90 of 90. Accessions matches for 88 of 91, because it stores UTC and the SRA XML does not | measured |
| Status | – | Accessions `Status`, `Updated`, `ReplacedBy` | Accessions, the only per-object change signal. It lists live, suppressed, unpublished and withdrawn runs; the Parquet holds live runs only | measured |
| Access | – | Accessions `Visibility`, Parquet `consent` | Accessions. 1,746,336 live runs are controlled access | measured |

Mark counts across the 46 rows: 42 measured, 4 read, 0 to-verify. The GEO rows rest on family
SOFT files fetched live; comparing them with the bulk GEO copy is still to do.

## Two probed studies

**PRJNA1027859, SRA-only.** Six experiments, one run each, 10x single-nucleus RNA-seq
(`TRANSCRIPTOMIC SINGLE CELL`, `PAIRED`, NovaSeq 6000). The SRA `STUDY` title and abstract
match the BioProject. The SRA sample has no `TITLE`. Its alias equals BioSample's "Sample
name", and BioSample's own title is the package's generic text. SRA's `SAMPLE_ATTRIBUTE` list
adds a `BioSampleModel` tag that BioSample does not list as an attribute. Every BioSample
attribute has a `harmonized_name`, because the submitter used the model-organism package. In
the Parquet, `sex` becomes the computed key `sex_calc` and `geo_loc_name: not applicable` is
dropped. The bulk BioProject and BioSample records equal the `efetch` replies.

**GSE60450, GEO.** Twelve GSMs, twelve SRX, one run each, single-end HiSeq 2000. The series
title, PubMed ID (25730472) and BioProject (PRJNA258286) match across SOFT, esummary, SRA and
BioProject. The SRA `center_name` is `GEO` on every object. BioSample keeps the GEO
characteristic keys as `attribute_name` and adds `harmonized_name` for 4 of the 6 keys.
`strain/background` and `immunophenotype` get none. GEO's `transcriptomic` is SRA's
`TRANSCRIPTOMIC`; the values differ only in case. In the Parquet each run also carries the GSM
as the attribute `geo_accession_exp`.

**A dropped package.** One `efetch db=sra` call for the 12 GSE60450 experiments returned 10
packages without an error. Three repeats of the same call returned all 12. A reader of the
SRA XML must check that every requested accession came back. seqforge's `fetch_records` does
not check this today.

## Does `!Sample_relation` carry the SRA and BioSample links

**Method.** The script `scripts/research/field_sources/soft_relations.py` downloaded the family
SOFT of all 90 Series touched by the GSM sample and read every sample in them: 60,724 GSMs
submitted from 2008 to 2025.

**Format.** Each link is one line holding one accession in a fixed URL:

```text
!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRX681985
!Sample_relation = BioSample: https://www.ncbi.nlm.nih.gov/biosample/SAMN02991530
```

Every SRA line held exactly one experiment accession: 41,995 `SRX` and 25 `ERX`. Every
BioSample line held one `SAMN`. No other URL form appeared. A GSM with several experiments gets
several `SRA:` lines. Other relation kinds seen were `Reanalyzed by: GSE…` and, for Roadmap
Epigenomics samples, `Named Annotation:`.

**Reliability.**

- **BioSample: nearly always.** 60,061 of the 60,339 samples of type `SRA` have a BioSample
  line (99.5%).
- **SRA: complete in most Series, absent in some.** In 74 of the 90 Series every sample of
  type `SRA` has an SRA line. Per sample the rate is lower, 42,020 of 60,339 (70%), because a
  few large Series dominate the count. The 16 Series with gaps fall into four groups:
  - Plate-based single-cell Series with one GSM per cell. GSE158435 links 3,248 of 16,491
    GSMs, and GSE94820 links none of 2,384.
  - Raw data held back: GSE285029 and GSE163634 have no SRA lines; GSE285029 has no
    BioSample lines either.
  - Roadmap Epigenomics (GSE16256, GSE17312, GSE19465) and 2009 to 2010 Series, partly linked.
  - SOFT missing a link SRA has. GSE175518 has no SRA line on any of its 942 GSMs, yet SRA
    holds a run under GSM5339608 in its SRP.
- **Family file and GSM record agree.** For all 72 sampled GSMs the family file lists the same
  SRX as the per-GSM record.
- **Multi-SRX GSMs exist.** GSE20301 (2010) has 6 GSMs with 3 or 4 SRX each, one per `SRA:`
  line, and 2 GSMs sequenced on 454 (GSM517436, GSM517440) of type `SRA` with no SRA line.
  GSE19456 has a GSM (GSM484820) with 2 SRX. Both were checked live with the same script.

## How lossy the GSM to SRX join is

**Method.** The script
[`scripts/research/gsm_srx_join.py`](https://github.com/liuhlab/liulab-data/blob/research/field-sources/scripts/research/gsm_srx_join.py)
produced the live numbers, run with `--per-year 4 --seed 23`. For each publication year from
2008 to 2025 it draws 4 random GSMs from
`gsm[ETYP] AND "high throughput sequencing"[PTYP] AND <year>[PDAT]`. That gives 72 GSMs in 90
Series. For each GSM it reads the GSM SOFT record, follows `elink gds→sra`, fetches every
linked SRX with `efetch db=sra`, and fetches the GSM's BioSample. The bulk numbers come from
`scripts/research/field_sources/accessions_check.sh` and `accessions_compare.py`, which look
for the GSM in the `Alias` column of `SRA_Accessions.tab`.

**Results.**

| Check | Count |
| --- | ---: |
| GSMs with no SRX link in SOFT | 9 of 72 |
| GSMs with more than one SRX | 0 of 63 |
| GSMs with more than one run | 6 of 63 |
| SRX found from the SOFT `Sample_relation` line | 63 of 63 |
| SRX found by Entrez `elink gds→sra` | 56 of 63, plus 1 that SOFT lacks |
| SRX found by the bulk `EXPERIMENT` alias | 54 of 63, plus 1 that SOFT lacks |
| SRX found by any bulk alias (experiment, run or sample) | 60 of 63, plus 1 that SOFT lacks |
| SOFT BioSample equals the SRA sample's BioSample | 56 of 63 |
| Library strategy, source, selection, instrument agree | 61, 63, 60, 62 of 63 |
| Taxid agrees | 61 of 63 |
| GEO characteristics found in BioSample, same key and value | 233 of 325 pairs (72%); 47 of 70 GSMs complete |
| GEO characteristics found in the SRA sample attributes | 204 of 260 pairs (78%); 41 of 63 GSMs complete |

**Why 9 GSMs have no SRX.**

- Raw data never reached SRA: two 2009 GSMs with no supplementary files, and three human
  GSMs from 2017, 2021 and 2025. One of them, GSE285029, says "Due to our institution's
  security policy, we are unable to provide raw data."
- Two 2011 Roadmap Epigenomics GSMs (GSE17312, GSE19465). They link to a project page
  outside NCBI, not to SRA.
- One 2021 single-cell GSM with no SRA line, in a Series that has an SRP.
- One 2022 GSM (GSM5339608) whose SRX exists, with the GSM as its alias, but SOFT omits the
  link. Only `elink`, an SRA search and the bulk aliases find it. Its only run has 8 spots.

**Why the alias joins miss some.** The 2008 to 2010 GEO deposits name the experiment freely,
such as `xrn1 rrp6 lsm1 pat1 Rep 4`. Their runs still carry `GSMn_1`, so the run alias
recovers six of them. Three stay lost to every alias route: a 2009 Roadmap GSM whose run
alias is a lab name (`renlab.2009-06-23.1`), a 2010 GSM that points at an ArrayExpress
deposit (`ERX000501`), and a 2014 GSM that points at an SRX another center submitted directly
to SRA. The old deposits also carry a second, older BioSample on the SRA side. `elink` misses
the same old and reused deposits. labdata's `experiments_for` follows `elink`, so it should miss them too
(inferred, not run).

**Across all of SRA.** The aliases in `SRA_Accessions.tab` name 5,620,204 distinct GSMs. Of
these, 5,619,754 map to exactly one SRX and 450 map to 2 to 9. The Parquet `sample_name`
names 5,461,662 distinct GSMs, fewer, because the Parquet lacks some runs. A join built on
the Parquet alone will report some Series as only partly joined when they join fully live.

**Why characteristics do not round-trip.** In the GSMs checked by hand:

- **The BioSample is older than the GEO record.** GSM1599675 has 5 characteristics in SOFT
  but only 1 in BioSample.
- **BioSample rewrites values.** `None` becomes `missing` (GSM6123703) and `Male` becomes
  `male` (GSM2632690).
- **The BioSample belongs to another submitter.** GSM6783145 uses an ENCODE BioSample with
  different attribute names.
- **Some early GEO characteristics have no key.** GSM340803 and GSM307158 (2008) carry a
  bare value with no `key:` part.

**Caveats.** The sample takes 4 GSMs from each year. Recent years hold most GSMs, so the pooled
rates over-weight old deposits. For 2011 onward the rates are 7 of 60 with no SRX and 1 of 53
missed by the experiment alias. With n = 72 the 95% interval on 12.5% is about 7% to 22%
(Wilson, inferred).

## The bulk files against the live services

| Bulk file | Compared | Result |
| --- | --- | --- |
| `bioproject.xml` | 62 BioProjects: title, description, publications, data types, dates | 62 of 62 equal; one title differs only by a double space |
| `biosample_set.xml.gz` | 95 BioSamples: title, taxid, package, last update, every attribute | 95 of 95 equal |
| Parquet | 91 runs: 12 ID and library fields, counts, dates, attributes | 90 found; every compared field equal. Counts are rounded down. 424 of 471 BioSample attribute pairs appear; 16 of the rest are empty values such as `not applicable` |
| `SRA_Accessions.tab` | 91 runs: status, spots, bases, BioSample, BioProject, release day | 91 found, all live and public; spots and bases 91 of 91 equal |

**Parquet coverage.** The script `scripts/research/field_sources/parquet_coverage.py` joined
the Parquet's 43,762,704 runs with the 43,022,633 live public runs in `SRA_Accessions.tab`.
971,528 live public runs are missing from the Parquet, spread over every year since 2007 and
mostly from 2021 on. One of the 91 sampled runs, SRR925577 (GSM1180151, 2013), is among them.
Every Parquet run appears in `SRA_Accessions.tab`.

## Scripts behind the numbers

All under `scripts/research/` on this branch:

| Script | Numbers |
| --- | --- |
| `gsm_srx_join.py --per-year 4 --seed 23` | the 72-GSM live sample and its join table |
| `field_sources/live_reference.py` | the live SRA, BioSample and BioProject values the bulk checks compare against |
| `field_sources/parquet_check.py` | Parquet against live, and the Parquet GSM counts |
| `field_sources/parquet_coverage.py` | the runs the Parquet lacks |
| `field_sources/accessions_check.sh`, `accessions_compare.py` | the bulk alias join, run counts and status tallies |
| `field_sources/xml_check.py` | bulk BioProject and BioSample against live |
| `field_sources/soft_relations.py` | the `!Sample_relation` survey and the GSE20301 and GSE19456 spot checks |

## Still to verify

- That the bulk GEO family SOFT files, once downloaded, equal the files fetched live here.

## Sources

- seqforge `fetch_records`, `src/seqforge/io/archive.py`, commit `17e39a8`, and seqforge's
  field list on issue #23.
- SRA XML schemas: <https://github.com/enasequence/schema/tree/master/src/main/resources/uk/ac/ebi/ena/sra/schema>
- BioSample and BioProject schemas:
  <https://www.ncbi.nlm.nih.gov/data_specs/schema/other/bioproject/Core.xsd>,
  <https://www.ncbi.nlm.nih.gov/biosample/docs/attributes/>
- SRA metadata Parquet columns:
  <https://www.ncbi.nlm.nih.gov/sra/docs/sra-cloud-based-metadata-table/>
- E-utilities: <https://www.ncbi.nlm.nih.gov/books/NBK25499/>
- Earlier notes: the bulk source survey
  ([discussion #17](https://github.com/liuhlab/liulab-data/discussions/17)), the GEO format
  note (#21) and the SRA-only papers note (#22).
