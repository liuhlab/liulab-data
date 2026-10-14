---
search:
  exclude: true
---

# Which source carries each study, sample, experiment and run field

Question: for each field a study needs, which source carries it? Where two sources carry the
same field, which one should win? And how much does the join from a GEO sample (GSM) to an SRA
experiment (SRX) lose?

Probes ran on 2026-10-14 against live NCBI services, with metadata only. Each table row has a
mark:

- **measured**: a live probe on 2026-10-14 printed it.
- **read**: an official page, a schema or an earlier note says it.
- **to-verify**: needs the bulk files (the SRA metadata Parquet, `SRA_Accessions.tab`,
  `bioproject.xml`, `biosample_set.xml`) before the claim can be trusted.

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
- **The join.** Of 72 random sequencing GSMs, 9 (12.5%) have no SRX. The other 63 each map to
  exactly one SRX. The SOFT `Sample_relation = SRA:` line finds all 63. Matching the SRA alias
  to the GSM finds 55, and the Entrez link finds 56. The misses are all deposits from 2008 to
  2010, plus one reused 2014 deposit.

## Field table

Source names used below:

- **SOFT**: the GEO family SOFT file, the format chosen in
  [the GEO format note](https://github.com/liuhlab/liulab-data/issues/21).
- **SRA XML**: `efetch db=sra`, one `EXPERIMENT_PACKAGE` per experiment. seqforge's
  `fetch_records` reads this.
- **Parquet**: NCBI's SRA metadata table on S3 (`sra.metadata`), one row per run.
- **Accessions**: `SRA_Accessions.tab`, one row per SRA object.
- **BioProject** and **BioSample**: `efetch` replies, or the bulk `bioproject.xml` and
  `biosample_set.xml`, which hold the same records (to-verify).
- **esummary**: `esummary db=gds` or `db=sra`.

A star (\*) marks a field seqforge's `fetch_records` reads today.

### Study

| Field | Sources | Winner and why | Mark |
| --- | --- | --- | --- |
| Study ID | GSE (SOFT). BioProject: SOFT `Series_relation`, SRA XML `STUDY` external ID\*, Parquet `bioproject`, Accessions `BioProject`. SRP: SRA XML, Parquet `sra_study`, Accessions `Study` | GSE when one exists, else BioProject. The SRP is SRA's internal ID; GEO omits it from 4 of 10 Series (#21) | measured |
| Title\* | SOFT `Series_title`, SRA XML `STUDY_TITLE`, BioProject `Title` | SOFT for GEO; BioProject otherwise. The other two are copies: identical on GSE60450 and PRJNA1027859. Parquet has no title | measured |
| Summary or abstract\* | SOFT `Series_summary`, SRA XML `STUDY_ABSTRACT`, BioProject `Description` | SOFT; BioProject otherwise. For GEO deposits SRA's abstract is the summary plus `Overall design:` and the design text, joined into one string | measured |
| Overall design | SOFT only | SOFT | read (#21) |
| Study type | SOFT `Series_type`, BioProject `ProjectDataTypeSet`\*, SRA XML `STUDY_TYPE` | SOFT for GEO, BioProject otherwise. The vocabularies differ, so keep both | measured |
| Contributors | SOFT `Series_contributor` only | SOFT. A SuperSeries has none of its own (#21) | read (#21) |
| Center\* | SRA XML `SUBMISSION center_name`, Parquet `center_name`, Accessions `Center`, BioProject owner | BioProject owner or SOFT contact institute. For every GEO deposit SRA says `GEO`, which names the broker, not the lab | measured |
| PubMed IDs | SOFT `Series_pubmed_id`, esummary `pubmedids`, BioProject `Publication`, SRA XML `STUDY_LINKS`, PubMed's `[si]` field | Union. All four agree on GSE60450 (25730472). For SRA-only studies NCBI rarely records one (#22) | measured |
| Dates | SOFT submission, last update, public date. BioProject `submitted`\* and `last_update`. esummary `pdat` | SOFT for GEO, BioProject otherwise | measured |
| SuperSeries and SubSeries | SOFT `Series_relation` only | SOFT; esummary leaves it empty (#21) | read (#21) |
| Organisms | SOFT `Series_sample_taxid`, BioProject target organism | Roll up from the samples | measured |

### Sample

| Field | Sources | Winner and why | Mark |
| --- | --- | --- | --- |
| Sample ID | GSM (SOFT). BioSample: SOFT `Sample_relation`, SRA XML `SAMPLE` external ID\*, Parquet `biosample`, Accessions `BioSample`. SRS: SRA XML, Parquet `sample_acc` | GSM, plus the BioSample from the SOFT line. 7 of 63 GSMs point at a different BioSample than their SRA sample. These are 2008 to 2010 deposits that got two BioSamples. Keep both | measured |
| Sample title\* | SOFT `Sample_title`, SRA XML `SAMPLE/TITLE`, BioSample `Title` | SOFT. For GEO the other two copy it. For SRA-only, SRA has no title and BioSample's title is generic ("Model organism or animal sample from …"), so use the sample name below | measured |
| Sample name or alias\* | SRA XML `SAMPLE alias`, BioSample "Sample name" ID, Parquet `sample_name` | SRA XML; for GEO it is the GSM | measured |
| Organism and taxid\* | SOFT `Sample_taxid_ch1`, SRA XML `TAXON_ID`, BioSample `Organism`, Parquet `organism` (a name, no taxid) | SOFT for GEO. 2 of 63 differ, and SRA has the more specific strain-level taxid each time. Keep both | measured |
| Characteristics\* | SOFT `Sample_characteristics_ch1` (`key: value` text), BioSample attributes, SRA XML `SAMPLE_ATTRIBUTE`, Parquet `attributes` and `jattr` | SOFT for GEO, BioSample for SRA-only. See the join section for the loss | measured |
| Harmonized attribute names\* | BioSample `harmonized_name` only | BioSample. On GSE60450, `gender` becomes `sex` and `developmental stage` becomes `dev_stage` | measured |
| Source name | SOFT `Sample_source_name_ch1`, BioSample attribute `source_name` | SOFT | measured |
| BioSample package\* | BioSample `Package` | BioSample | measured |
| Protocols, molecule, data processing | SOFT only | SOFT | read (#21) |
| Sample files | SOFT `Sample_supplementary_file_*` | SOFT | read (#21) |
| Other Series of the sample | SOFT `Sample_series_id` | SOFT | read (#21) |

### Experiment

| Field | Sources | Winner and why | Mark |
| --- | --- | --- | --- |
| Experiment ID and alias\* | SRA XML, Parquet `experiment`, Accessions `Alias` | SRA XML. For GEO deposits since 2011 the alias is the GSM | measured |
| Experiment title\* | SRA XML `TITLE` | For GEO it is generated: "GSM…: sample title; organism; strategy". Low value | measured |
| Library strategy, source, selection\* | SOFT `Sample_library_*`, SRA XML, Parquet `assay_type`, `librarysource`, `libraryselection` | SRA. Agree for 61, 63 and 60 of 63 GSMs, ignoring case. Every mismatch is an old or reused deposit | measured; Parquet to-verify |
| Library layout | SRA XML, esummary sra, Parquet `librarylayout` | SRA. GEO has no layout | measured; Parquet to-verify |
| Library name\* | SRA XML `LIBRARY_NAME`, Parquet `library_name` | SRA. Often empty for GEO deposits | measured |
| Instrument\* | SOFT `Sample_instrument_model`, SRA XML, Parquet `instrument` | SRA. Agree for 62 of 63 | measured; Parquet to-verify |
| Design description\* | SRA XML `DESIGN_DESCRIPTION` | SRA. Empty for GEO deposits | measured |
| Library construction protocol\* | SRA XML `LIBRARY_CONSTRUCTION_PROTOCOL`, SOFT `Sample_extract_protocol_ch1` | SOFT for GEO; SRA's text is a copy. SRA otherwise. Not in the Parquet | measured |
| GEO platform (GPL) | SOFT only | SOFT | read (#21) |

### Run

| Field | Sources | Winner and why | Mark |
| --- | --- | --- | --- |
| Run ID and alias\* | SRA XML `RUN`, Parquet `acc`, Accessions | Accessions for the full list; SRA XML for one study | measured; Accessions to-verify |
| Spots and bases | SRA XML `total_spots`, `total_bases`; esummary sra; Accessions `Spots`, `Bases`; Parquet `mbases` and `avgspotlen` (rounded, no spot count) | Accessions for bulk: exact counts. SRA XML and esummary agree on the probed runs | measured; Accessions to-verify |
| Reads per spot, mean read length | SRA XML `Statistics/Read` only | SRA XML. PRJNA1027859 shows 28 + 94 bases, the 10x read layout | measured |
| Size in bytes | SRA XML `size`, esummary, Parquet `mbytes` | SRA XML | measured |
| Submitted files\* | SRA XML `SRAFile supertype="Original"`: name, md5, size, URI | SRA XML only | measured |
| Release date | SRA XML `published`, Parquet `releasedate`, Accessions `Published` | Accessions | measured; others to-verify |
| Status, change time | Accessions `Status`, `Updated`, `ReplacedBy`; SRA XML `is_public` | Accessions, the only per-object change signal (#17) | to-verify |
| Access (public or controlled) | Parquet `consent`, Accessions `Visibility` | Accessions | to-verify |

Mark counts across the 39 rows: 24 measured, 7 read (from the GEO format note), 6 measured on
the live sources with a Parquet or Accessions column still to verify, and 2 to-verify.

## Two probed studies

**PRJNA1027859, SRA-only.** Six experiments, one run each, 10x single-nucleus RNA-seq
(`TRANSCRIPTOMIC SINGLE CELL`, `PAIRED`, NovaSeq 6000). The SRA `STUDY` title and abstract
match the BioProject. The SRA sample has no `TITLE`. Its alias equals BioSample's "Sample
name", and BioSample's own title is the package's generic text. SRA's `SAMPLE_ATTRIBUTE` list
adds a `BioSampleModel` tag that BioSample does not list as an attribute. Every BioSample
attribute has a `harmonized_name`, because the submitter used the model-organism package. The
`center_name` is the university, as expected for a direct submission.

**GSE60450, GEO.** Twelve GSMs, twelve SRX, one run each, single-end HiSeq 2000. The series
title, PubMed ID (25730472) and BioProject (PRJNA258286) match across SOFT, esummary, SRA and
BioProject. The SRA `center_name` is `GEO` on every object. BioSample keeps the GEO
characteristic keys as `attribute_name` and adds `harmonized_name` for 4 of the 6 keys.
`strain/background` and `immunophenotype` get none. GEO's `transcriptomic` is SRA's
`TRANSCRIPTOMIC`; the values differ only in case.

**A dropped package.** One `efetch db=sra` call for the 12 GSE60450 experiments returned 10
packages without an error. Three repeats of the same call returned all 12. A reader of the
SRA XML must check that every requested accession came back. seqforge's `fetch_records` does
not check this today.

## How lossy the GSM to SRX join is

**Method.** The script
[`scripts/research/gsm_srx_join.py`](https://github.com/liuhlab/liulab-data/blob/research/field-sources/scripts/research/gsm_srx_join.py)
produced every number in this section, run with `--per-year 4 --seed 23`. For each
publication year from 2008 to 2025 it draws 4 random GSMs from
`gsm[ETYP] AND "high throughput sequencing"[PTYP] AND <year>[PDAT]`. That gives 72 GSMs in 90
Series. For each GSM it reads the GSM SOFT record, follows `elink gds→sra`, fetches every
linked SRX with `efetch db=sra`, and fetches the GSM's BioSample. The script compares IDs and
fields. It refetches any package that `efetch` drops.

**Results.**

| Check | Count |
| --- | ---: |
| GSMs with no SRX link in SOFT | 9 of 72 |
| GSMs with more than one SRX | 0 of 63 |
| GSMs with more than one run | 6 of 63 |
| SRX found from the SOFT `Sample_relation` line | 63 of 63 |
| SRX found by Entrez `elink gds→sra` | 56 of 63, plus 1 that SOFT lacks |
| SRX whose alias or sample GEO ID is the GSM | 55 of 63 |
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
  link. Only `elink` and an SRA search find it. Its only run has 8 spots.

**Why the alias join misses 8.** Seven GSMs from 2008 to 2010 point at SRX records whose
alias is a free name such as `xrn1 rrp6 lsm1 pat1 Rep 4`, not the GSM. Their SRA samples also
carry a second, older BioSample. One of the seven, GSM548904, points at an ArrayExpress
deposit (`ERX000501`). The eighth is a 2014 GSM that points at an SRX another center
submitted directly to SRA. So a bulk join on `Alias = GSM` in `SRA_Accessions.tab` should
miss about 1 in 8 GSMs that have an SRX in this year-balanced sample. It should miss few after
2010: 1 of the 53 from 2011 on. The same eight are what `elink` misses. labdata's
`experiments_for` follows `elink`, so it should miss them too (inferred, not run).

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
missed by the alias join. With n = 72 the 95% interval on 12.5% is about 7% to 22% (Wilson,
inferred). All results come from live services, not the bulk files.

## Still to verify against the bulk files

- That the Parquet `assay_type`, `librarysource`, `libraryselection`, `librarylayout` and
  `instrument` equal the SRA XML values, run for run.
- That `SRA_Accessions.tab` `Spots` and `Bases` equal the SRA XML totals, and how its
  `Status`, `Visibility` and `ReplacedBy` behave.
- That the `Alias = GSM` join in `SRA_Accessions.tab` gives the same 55 of 63 as the live
  check.
- That the Parquet `attributes` and `jattr` equal the BioSample attributes, harmonized names
  included.
- That bulk `bioproject.xml` and `biosample_set.xml` records match the `efetch` replies.

## Sources

- seqforge `fetch_records`, `src/seqforge/io/archive.py`, commit `17e39a8`: the fields it
  reads.
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
