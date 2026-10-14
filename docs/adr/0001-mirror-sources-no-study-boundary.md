---
search:
  exclude: true
---

# Mirror each source and draw no study boundary

labdata mirrors each source's own records faithfully: GEO SuperSeries, Series, Sample and
Platform; SRA study, experiment and run; BioProject; BioSample. It keeps every link the sources
carry between them, such as SubSeries to SuperSeries, Series to BioProject, GEO sample to SRA
experiment, and record to PubMed ID. It draws no study boundary. Grouping records into studies
happens downstream in seqforge. Whether a model does that grouping is proposed in seqforge's
[Metadata for all of GEO](https://github.com/liuhlab/seqforge/discussions/500) discussion and
not yet decided.

## Why

Where a study starts and stops varies paper by paper. Authors use SuperSeries, Series and
BioProjects differently, so no fixed level is the study. Deciding it takes reading, and labdata
is a programmatic store with no LLM in it.

## Considered options

- **A SubSeries is a study.** Fails when authors split one study across SubSeries, or file
  unrelated work under one SuperSeries.
- **A BioProject is a study.** Same problem: a BioProject can hold one study or several.
- **Bound studies by paper.** GEO-less, SRA-only submissions rarely record their paper in NCBI:
  7.5% of a sample of 200 such BioProjects did.

## Consequences

- No Study table and no grouping helper. Consumers that want studies group the records
  themselves, on demand.
- Cross-source joins, such as GEO sample to SRA experiment, stay labdata's job: they are links,
  not boundaries.
- Each source keeps its own field names; there is no shared-name layer. Downstream needs to know
  which source a value came from, and a shared name would be a second copy that can drift.
