---
search:
  exclude: true
---

# Where SRA-only submissions record their papers

Question: for a sequencing submission with no GEO Series, where is its paper recorded, and how
often? Does seqforge's demo dataset, PRJNA1027859 (SRA study SRP502277), have one?

All checks ran on 2026-10-14 against live NCBI E-utilities, Europe PMC and ENA, with metadata
only. Each claim is tagged **measured** (counted from a query), **read** (seen in a record or
schema) or **inferred**.

## Answer

- The submitter-side records rarely carry a paper. Across GEO-less sequencing BioProjects,
  about 7% have a PubMed or DOI link anywhere in NCBI's structured metadata. GEO-linked
  BioProjects have one far more often (77%).
- The paper usually exists anyway. About half of the same projects are cited by accession in
  Europe PMC's literature index, and the earliest citing article is usually the paper that
  produced the data.
- The NCBI routes are not independent. A BioProject `Publication` element shows up in the
  bioproject→pubmed link, the sra→pubmed link and the SRA `STUDY_LINKS` together. PubMed's own
  databank field (`[si]`) is a separate route, which no Entrez link exposes.
- The demo dataset has a paper: Cell Genomics 2024, PMID 39637862 (PMC11701262). Only the
  publication side records it, in PubMed's databank field and in Europe PMC. The BioProject,
  the SRA study and every Entrez link carry nothing.

## The demo dataset, PRJNA1027859

| Route | Result | Evidence |
| --- | --- | --- |
| BioProject XML (efetch) `Publication` | none; the record has not changed since registration (2023-10-13) | read |
| BioProject esummary | no publication field | read |
| elink bioproject→pubmed | no link set | read |
| elink sra→pubmed (all 6 SRA experiments) | no link set | read |
| SRA `STUDY` XML | no `STUDY_LINKS` element | read |
| PubMed databank field, `PRJNA1027859[si]` | PMID 39637862; record shows `SI - BioProject/PRJNA1027859` | read |
| elink pubmed→bioproject from 39637862 | no link set | read |
| Europe PMC `ACCESSION_ID:PRJNA1027859` | 39637862 (Cell Genomics 2024), plus 41417737 (STAR Protocols 2026) and 41426962 (microPublication Biology 2025), which reuse the data | read |
| Europe PMC search for `SRP502277` | no hits | read |
| ENA study XML | only file cross-references, no PubMed | read |

The paper is "Adult single-nucleus neuronal transcriptomes of insulin signaling mutants reveal
regulators of behavior and learning", Cell Genomics 2024, doi:10.1016/j.xgen.2024.100720. Its
title differs from the BioProject title, so a title search does not find it (read).

## Census: Entrez link counts

BioProject search filters, registration dates 2019-01-01 to 2022-12-31 (measured):

| Set | BioProjects | With a bioproject→pubmed link |
| --- | ---: | ---: |
| Has SRA data, no GEO link | 177,131 | 8,528 (4.8%) |
| Same, project data type "raw sequence reads" | 92,540 | 2,307 (2.5%) |
| Has SRA data and a GEO link | 63,075 | 48,494 (77%) |

Query: `bioproject_sra[filter] AND 2019/01/01:2022/12/31[DATE]`, combined with
`bioproject_gds[filter]`, `"raw sequence reads"[DTPE]` and `bioproject_pubmed[filter]`. The
date window leaves at least three years for a paper to appear.

## Sample: every route on 200 projects

Method: all 92,540 UIDs from the "raw sequence reads, no GEO" set above, then a random sample of
200 (Python `random.sample`, seed 22). For each project the script tested every route below. It
used the BioProject accession, plus the SRP accession taken from one experiment's SRA XML. Every
project had at least one SRA experiment.

| Route | Projects with a paper | Share |
| --- | ---: | ---: |
| BioProject XML `Publication` (PubMed ID or DOI) | 8 | 4.0% |
| elink bioproject→pubmed | 5 | 2.5% |
| elink sra→pubmed (up to 200 experiments) | 6 | 3.0% |
| SRA `STUDY_LINKS` with a PubMed cross-reference | 6 | 3.0% |
| PubMed databank field, `ACC[si]` | 6 | 3.0% |
| Any of the five NCBI routes above | 15 | 7.5% |
| Europe PMC `ACCESSION_ID:` (text-mined accession) | 62 | 31% |
| Europe PMC plain search for the accession | 98 | 49% |
| ENA study PubMed cross-reference | 2 | 1.0% |
| Any route | 102 | 51% |

All counts are measured. What they show:

- **The BioProject `Publication` element feeds the Entrez links.** The 5 elink hits are exactly
  the 5 projects whose `Publication` holds a PubMed ID. The other 3 hold only a DOI and get no
  link. The sra→pubmed link and the SRA `STUDY_LINKS` agree with each other in all 6 cases (5
  overlap the BioProject ones). Measured overlap; the mechanism is inferred.
- **PubMed's databank field is a separate route.** Its 6 hits share no project with the
  `Publication` route (measured). The publisher deposits the accession with the article, so
  the link lives on the PubMed record, not on the BioProject (inferred from the demo, where
  only the PubMed side records it).
- **Europe PMC finds the most, with some noise.** Its plain search covers abstracts and
  open-access full text, so later papers that reuse a dataset also match. One glioblastoma
  project matched 12 articles. In a spot check of 20 projects, the earliest matching article was
  the data-generating paper in 18 or 19 (judged by title, topic and year; inferred). The
  `ACCESSION_ID:` field is narrower: it holds only accessions Europe PMC's text-mining tagged.
- **ENA adds nothing** for NCBI-submitted projects: 2 hits, both already linked at NCBI
  (measured).

## Caveats

- One sample of 200; a 95% interval on 7.5% is roughly 4–11%, and on 49% roughly 42–56%
  (normal approximation, inferred).
- The "raw sequence reads" set includes pathogen surveillance and genome-assembly read sets.
  These seldom have a dedicated paper, which pulls the shares down (inferred).
- A Europe PMC hit is a citation of the accession, not a declared link. Taking the earliest
  article as "the paper" is a heuristic, and articles without open full text are missed
  (inferred).
- Europe PMC and ENA are non-NCBI sources; they are used here only to measure what the NCBI
  records miss.

## Sources

- BioProject XML schema, `Publication` element with `DbType` `ePubmed` or `eDOI`:
  <https://www.ncbi.nlm.nih.gov/data_specs/schema/other/bioproject/Core.xsd>
- SRA study schema, `STUDY_LINKS` with `XREF_LINK` and `URL_LINK`:
  <https://github.com/enasequence/schema/blob/master/src/main/resources/uk/ac/ebi/ena/sra/schema/SRA.study.xsd>
- E-utilities (esearch, efetch, elink, einfo for the `bioproject` fields):
  <https://www.ncbi.nlm.nih.gov/books/NBK25499/>
- PubMed secondary source ID field `[si]`:
  <https://pubmed.ncbi.nlm.nih.gov/help/>
- Europe PMC REST search (`ACCESSION_ID` field) and Annotations API:
  <https://europepmc.org/RestfulWebService>
- ENA browser API: <https://www.ebi.ac.uk/ena/browser/api/>
- Demo paper: <https://pubmed.ncbi.nlm.nih.gov/39637862/>
